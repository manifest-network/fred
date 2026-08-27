package provisioner

import (
	"cmp"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"math/rand/v2"
	"reflect"
	"runtime/debug"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/metrics"
	"github.com/manifest-network/fred/internal/provisioner/operation"
	"github.com/manifest-network/fred/internal/provisioner/payload"
	"github.com/manifest-network/fred/internal/provisioner/placement"
)

// Default concurrency limits for reconciliation.
const (
	// DefaultReconcileWorkers is the default number of concurrent workers for
	// processing leases and orphans during reconciliation.
	DefaultReconcileWorkers = 10
)

// chainConfirmTimeout bounds ONE per-candidate lease lookup in
// queryLeaseLiveness, shared by destructive cleanup confirmation and
// conservative placement-marker retirement.
//
// The reconcile context is the process lifetime — Start passes it straight
// through to every sweep — and neither the chain client nor gRPC imposes a
// per-RPC deadline of its own, so without this an unanswered Lease query stalls
// the sweep that issued it while ReconcileAll's CAS flag makes every later tick
// a no-op. The conservative path this guard exists to reach (keep the state or
// marker and retry next sweep; destructive callers also count chain_error) is
// only reachable if the call returns.
//
// Not a config knob: it is a liveness backstop on a single point query, not a
// tuning parameter, and it matches the hardcoded budgets the chain client
// already applies to Ping and gas simulation. Generous on purpose — a lease
// lookup that takes ten seconds means the node is in trouble, and skipping a
// cycle of cleanup is the correct response either way.
const chainConfirmTimeout = 10 * time.Second

// errLeaseAlreadyInFlight indicates the lease is already being provisioned.
// This is not a real error - the caller should not treat it as a failure.
var errLeaseAlreadyInFlight = errors.New("lease already in-flight")

// errPlacementSnapshotStale means a placement mutation crossed the inventory
// boundary before the reconciler's write-ahead attempt could commit. The lease
// is retried from a newer snapshot; no backend call was made.
var errPlacementSnapshotStale = errors.New("placement changed after inventory snapshot")

var errTrackerSnapshotStale = errors.New("in-flight operation changed after inventory snapshot")

// reconcileActionAuthority carries the exact process-local and durable
// capabilities that authorize one action planned from a fleet snapshot. The
// typed fields are intentionally opaque and safe-zero. Numeric revisions exist
// only for compatibility with legacy test doubles while callers migrate.
type reconcileActionAuthority struct {
	leaseClaim        operation.LeaseClaim
	baseline          placement.AdmissionBaseline
	recordlessScope   placement.AdmissionScope
	allowRecordless   bool
	eligibleBackends  map[string]struct{}
	ownedPlacement    placement.RecordRevision
	ownedBackend      string
	placementRevision uint64
	trackerRevision   uint64
}

// errPayloadNotAvailable indicates the payload required for provisioning is
// not in the store. This is a permanent failure — the lease cannot be
// re-provisioned and should be closed.
var errPayloadNotAvailable = errors.New("payload not available")

// ReconcilerPayloads is the payload capability consumed by reconciliation.
// Lifecycle coordination lives in operation.Registry; payload persistence is a
// separate concern and deliberately does not expose legacy tracker mutation.
type ReconcilerPayloads interface {
	HasPayload(leaseUUID string) (bool, error)
	PayloadStore() *payload.Store
}

// ReconcilerRuntime is the production composition boundary. A runtime exposes
// only the reconciler's lifecycle port plus payload reads; it cannot hand the
// reconciler the concrete registry or a second implementation of lifecycle
// state.
type ReconcilerRuntime interface {
	ReconcilerPayloads
	ReconcilerOperations() ReconcilerOperations
}

// ReconcilerChainClient defines the chain operations needed by the reconciler.
type ReconcilerChainClient interface {
	// GetLease returns a lease by UUID regardless of state, or (nil, nil) when
	// the chain has no record of it. The sweep's two list queries are filtered
	// to PENDING/ACTIVE, so this is the only way to tell a terminal lease from
	// one the chain never knew — the distinction every destructive pass rests
	// on (ENG-654, see classifyLease).
	GetLease(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error)
	GetPendingLeases(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error)
	GetActiveLeasesByProvider(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error)
	AcknowledgeLeases(ctx context.Context, leaseUUIDs []string) (uint64, []string, error)
	RejectLeases(ctx context.Context, leaseUUIDs []string, reason string) (uint64, []string, error)
	CloseLeases(ctx context.Context, leaseUUIDs []string, reason string) (uint64, []string, error)
}

// Reconciler performs level-triggered reconciliation between chain state and backend state.
// It ensures consistency by comparing current state rather than replaying events.
type Reconciler struct {
	providerUUID       string
	callbackBaseURL    string
	chainClient        ReconcilerChainClient
	acknowledger       Acknowledger // Routes acks through the batcher for parallel signing
	backendRouter      BackendRouter
	payloads           ReconcilerPayloads
	tracker            ReconcilerTracker // Legacy test-double compatibility only.
	operations         ReconcilerOperations
	placementView      PlacementView
	legacyPlacement    PlacementStore // Non-nil only through newReconciler.
	placementAuthority ReconcilerPlacement
	startEvents        ProvisionStartEventSink

	interval               time.Duration
	maxWorkers             int         // Maximum concurrent workers for lease processing
	maxReprovisionAttempts int         // Max re-provision attempts before rejecting
	reconciling            atomic.Bool // Non-blocking flag to prevent concurrent reconciliation
	placementSweepSeen     atomic.Bool // True while a durable baseline matches the configured backend topology.
	// placementAbsenceUntrusted narrows exceptions to the process-wide absence
	// proof. A positive observation excluded from placement sync because its
	// outbound operation straddled the inventory boundary must remain untrusted
	// for that lease until a later durable observation or complete inventory
	// settles it. ReconcileAll is serialized, and workers only read this map after
	// the sweep has finished updating it.
	// Each marker retains the backend(s) whose positive observation was excluded.
	// A later partial sweep may retire a single-backend marker only when that same
	// backend freshly reports the lease and matches the durable confirmed record.
	placementAbsenceUntrusted map[string]map[string]struct{}
	// ambiguousPlacements quarantines leases positively reported by multiple
	// backends. It persists across partial sweeps so an already-armed absence
	// latch cannot turn a temporarily hidden conflict into a third provision.
	ambiguousPlacements map[string][]string
}

// DefaultMaxReprovisionAttempts is the default number of re-provision attempts
// before rejecting a lease whose containers keep failing.
const DefaultMaxReprovisionAttempts = 3

// ReconcilerConfig configures the reconciler.
type ReconcilerConfig struct {
	ProviderUUID           string
	CallbackBaseURL        string
	Interval               time.Duration // How often to run periodic reconciliation
	MaxWorkers             int           // Maximum concurrent workers (default: 10)
	MaxReprovisionAttempts int           // Max re-provision attempts before rejecting (default: 3)
	StartEvents            ProvisionStartEventSink
}

// NewReconciler creates the production reconciler. Placement authority and the
// shared operation registry are mandatory: every supported deployment uses a
// multi-backend router, so constructing a reconciler that can issue an
// unrecorded or unfenced backend mutation is invalid.
func NewReconciler(
	cfg ReconcilerConfig,
	chainClient ReconcilerChainClient,
	acknowledger Acknowledger,
	backendRouter BackendRouter,
	runtime ReconcilerRuntime,
	placementStore ReconcilerPlacement,
) (*Reconciler, error) {
	if isNilReconcilerDependency(runtime) {
		return nil, errors.New("reconciler runtime is required")
	}
	if isNilReconcilerDependency(placementStore) {
		return nil, errors.New("placement authority store is required")
	}
	operations := runtime.ReconcilerOperations()
	if isNilReconcilerDependency(operations) {
		return nil, errors.New("reconciler operations are required")
	}
	reconciler, err := buildReconciler(
		cfg, chainClient, acknowledger, backendRouter,
		runtime, operations, nil, placementStore, nil, placementStore,
	)
	if err != nil {
		return nil, err
	}
	if err := placementStore.ConfigureBackendTopology(backendTopologyNames(backendRouter)); err != nil {
		return nil, fmt.Errorf("configure reconciler backend topology: %w", err)
	}
	reconciler.placementSweepSeen.Store(placementStore.CurrentAdmissionBaseline().Valid())
	return reconciler, nil
}

// newReconciler retains the pre-typed dependency shape for isolated tests of
// read-only planning and legacy safety barriers. Production composition must
// use NewReconciler, whose signature makes both authorities mandatory.
func newReconciler(
	cfg ReconcilerConfig,
	chainClient ReconcilerChainClient,
	acknowledger Acknowledger,
	backendRouter BackendRouter,
	tracker ReconcilerTracker,
	placementStore PlacementStore,
) (*Reconciler, error) {
	return buildReconciler(
		cfg, chainClient, acknowledger, backendRouter,
		tracker, operationRegistryFromReconcilerTracker(tracker), tracker,
		placementStore, placementStore, nil,
	)
}

func buildReconciler(
	cfg ReconcilerConfig,
	chainClient ReconcilerChainClient,
	acknowledger Acknowledger,
	backendRouter BackendRouter,
	payloads ReconcilerPayloads,
	operations ReconcilerOperations,
	tracker ReconcilerTracker,
	placementView PlacementView,
	legacyPlacement PlacementStore,
	placementAuthority ReconcilerPlacement,
) (*Reconciler, error) {
	if isNilReconcilerDependency(operations) {
		operations = nil
	}
	if chainClient == nil {
		return nil, errors.New("chain client is required")
	}
	if acknowledger == nil {
		return nil, errors.New("acknowledger is required")
	}
	if backendRouter == nil {
		return nil, errors.New("backend router is required")
	}
	if placementView != nil && tracker == nil && operations == nil {
		return nil, errors.New("in-flight tracker is required when placement store is enabled")
	}
	if cfg.ProviderUUID == "" {
		return nil, errors.New("provider UUID is required")
	}
	if cfg.CallbackBaseURL == "" {
		return nil, errors.New("callback base URL is required")
	}

	// Apply defaults using cmp.Or (returns first non-zero value)
	interval := cmp.Or(cfg.Interval, 5*time.Minute)
	maxWorkers := cmp.Or(max(cfg.MaxWorkers, 0), DefaultReconcileWorkers)
	maxReprovision := cmp.Or(max(cfg.MaxReprovisionAttempts, 0), DefaultMaxReprovisionAttempts)
	startEvents := cfg.StartEvents
	if isNilCapability(startEvents) {
		startEvents = nil
	}

	return &Reconciler{
		providerUUID:              cfg.ProviderUUID,
		callbackBaseURL:           cfg.CallbackBaseURL,
		chainClient:               chainClient,
		acknowledger:              acknowledger,
		backendRouter:             backendRouter,
		payloads:                  payloads,
		tracker:                   tracker,
		operations:                operations,
		placementView:             placementView,
		legacyPlacement:           legacyPlacement,
		placementAuthority:        placementAuthority,
		startEvents:               startEvents,
		interval:                  interval,
		maxWorkers:                maxWorkers,
		maxReprovisionAttempts:    maxReprovision,
		placementAbsenceUntrusted: make(map[string]map[string]struct{}),
		ambiguousPlacements:       make(map[string][]string),
	}, nil
}

func isNilReconcilerDependency(dependency any) bool {
	if dependency == nil {
		return true
	}
	value := reflect.ValueOf(dependency)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

func operationRegistryFromReconcilerTracker(tracker ReconcilerTracker) *operation.Registry {
	provider, ok := tracker.(interface {
		Operations() *operation.Registry
	})
	if !ok {
		return nil
	}
	return provider.Operations()
}

func (r *Reconciler) isInFlight(leaseUUID string) bool {
	if r.operations != nil {
		return r.operations.Contains(leaseUUID)
	}
	return r.tracker != nil && r.tracker.IsInFlight(leaseUUID)
}

// claimLeaseAction acquires the exact process-local capability that fences one
// reconciliation side effect against lifecycle events. A claim is evaluated
// against the inventory-start boundary: an operation that began, completed, or
// merely held a claim after that boundary makes the observation stale. Holding
// the returned release function then prevents a delayed event from starting
// until the caller's backend or placement mutation is complete.
//
// The raw tracker branch exists only for the legacy test constructor. The
// exported production constructor always supplies Registry capabilities.
func (r *Reconciler) claimLeaseAction(
	leaseUUID string,
	operationSnapshot operation.TrackerSnapshot,
	trackerSnapshotRevision uint64,
) (release func(), acquired bool) {
	if r.operations != nil {
		if !operationSnapshot.Valid() {
			return nil, false
		}
		result := r.operations.TryClaimLease(leaseUUID, operationSnapshot)
		if !result.Acquired() {
			return nil, false
		}
		claim := result.Claim()
		return func() {
			if !r.operations.ReleaseLease(claim) {
				slog.Error("failed to release reconciler lease action claim",
					"lease_uuid", leaseUUID)
			}
		}, true
	}
	if r.tracker != nil {
		claimed, _ := r.tracker.TryClaimLeaseActionIfNotNewer(
			leaseUUID, trackerSnapshotRevision,
		)
		if !claimed {
			return nil, false
		}
		return func() { r.tracker.ReleaseLeaseAction(leaseUUID) }, true
	}
	return func() {}, true
}

func (r *Reconciler) payloadStore() *payload.Store {
	if r.payloads == nil {
		return nil
	}
	return r.payloads.PayloadStore()
}

// ReconcileAll performs a full reconciliation between chain state and backend state.
// This is the core level-triggered reconciliation logic.
//
// State Matrix:
// | Chain State | Backend State | Action |
// |-------------|---------------|--------|
// | PENDING     | Not provisioned | Start provisioning |
// | PENDING     | Provisioning (in progress) | Nothing (wait for callback) |
// | PENDING     | Provisioned + ready (in-flight) | Skip; main flow owns the ack |
// | PENDING     | Provisioned + ready | Acknowledge lease |
// | PENDING     | Provisioned + failed | Reject lease on chain |
// | ACTIVE      | Provisioned + ready | Nothing (healthy) |
// | ACTIVE      | Provisioned + failed | Re-provision (close after max attempts) |
// | ACTIVE      | Not provisioned | Anomaly: re-provision with payload |
// | Not found   | Provisioned | Orphan: Deprovision |
//
// Every row above is conditioned on the sweep being able to identify the
// lease's owning backend. When a backend does not answer, its leases are
// DEFERRED because acting on a lease fred cannot see risks re-provisioning it
// onto a healthy peer and laying an empty volume over live data (ENG-356).
// Before the first topology-bound baseline, incomplete inventory withholds new
// backend effects globally. After bootstrap it narrows admission instead: a
// recordless PENDING lease may use only a backend that answered both inventory
// endpoints, while recorded work remains pinned to its exact owner.
// See deferLease.
func (r *Reconciler) ReconcileAll(ctx context.Context) (retErr error) {
	// Use atomic flag to prevent concurrent reconciliation without blocking.
	// If reconciliation is already in progress, skip this run.
	if !r.reconciling.CompareAndSwap(false, true) {
		slog.Debug("reconciliation already in progress, skipping")
		return nil
	}
	defer r.reconciling.Store(false)

	// Check for cancellation before starting
	if err := ctx.Err(); err != nil {
		return err
	}

	// Track reconciliation duration and outcome
	startTime := time.Now()
	defer func() {
		metrics.ReconciliationDuration.Observe(time.Since(startTime).Seconds())
		if retErr != nil && !errors.Is(retErr, context.Canceled) {
			metrics.ReconciliationTotal.WithLabelValues(metrics.OutcomeError).Inc()
		}
	}()

	slog.Info("starting reconciliation", "provider_uuid", r.providerUUID)
	typedCoordination := r.operations != nil && r.placementAuthority != nil
	// Sweep completeness describes current observability only. Beginning the
	// typed inventory session invalidates older projection proofs, while the
	// separately persisted topology baseline remains valid through a transient
	// backend outage.
	metrics.ReconcilerSweepComplete.Set(0)

	// Capture both operation boundaries before the first chain/backend read.
	// Every lifecycle decision below must be based on state no older than this
	// boundary; an operation that starts or finishes afterward is deferred by its
	// placement revision, tracker tombstone, or live action claim.
	var placementSnapshotRevision uint64
	if r.legacyPlacement != nil {
		// Raw revision handles exist only for the compatibility adapter used by
		// legacy unit tests. Production never retains this interface.
		placementSnapshotRevision = r.legacyPlacement.BeginInventorySnapshot()
		defer r.legacyPlacement.EndInventorySnapshot(placementSnapshotRevision)
	}
	var inventoryFence placement.InventoryFence
	if r.placementAuthority != nil {
		inventoryFence = r.placementAuthority.BeginInventorySession()
		defer r.placementAuthority.EndInventorySession(inventoryFence)
	}
	inFlightAtSnapshot := make(map[string]struct{})
	var (
		operationSnapshot       operation.TrackerSnapshot
		trackerSnapshotRevision uint64
	)
	if typedCoordination {
		operationSnapshot = r.operations.Snapshot()
		for _, leaseUUID := range r.operations.LeaseUUIDs() {
			inFlightAtSnapshot[leaseUUID] = struct{}{}
		}
	} else if r.tracker != nil {
		trackerSnapshotRevision = r.tracker.SnapshotMutationRevision()
		for _, leaseUUID := range r.tracker.GetInFlightLeases() {
			inFlightAtSnapshot[leaseUUID] = struct{}{}
		}
	}

	// Collection is read-only. Its facts become authority only after the atomic
	// placement projection below commits successfully.
	inventory, err := r.collectInventory(ctx)
	if err != nil {
		return err
	}
	chainLeases := inventory.chainLeases

	slog.Info("fetched chain leases",
		"pending", inventory.pending,
		"active", inventory.active,
	)

	// 2. Get provisions from ALL backends (in parallel). An unanswered backend
	// does not abort collection or the separately safe reconciliation passes.
	// Work tied to that backend remains deferred; after a durable topology
	// baseline exists, genuinely new leases may still use backends that answered
	// both inventories (ENG-356, ENG-632).
	snapshot := inventory.fleet
	allProvisions := snapshot.provisions

	if snapshot.complete {
		slog.Info("fetched backend provisions", "total", len(allProvisions))
	} else {
		slog.Warn("reconciling with an incomplete fleet view; node-affine work is deferred and new placement is restricted to fully answering backends",
			"total", len(allProvisions),
			"unanswered", snapshot.unansweredBackends(),
		)
	}

	// Retained leases also pin a backend (restore affinity, ENG-333). Fetch them
	// only when placement tracking is enabled: the results feed solely the
	// placement sync and the pruner below, both of which no-op without a
	// placement store. Skipping avoids pointless per-backend /retentions calls and
	// log noise on placement-disabled deployments.
	allRetentions := inventory.retentions
	retentionsAnswered := inventory.retentionsAnswered
	retentionsReportedByBackend := inventory.retentionsReportedByBackend
	if r.placementView != nil {
		slog.Info("fetched backend retentions",
			"total", len(allRetentions),
			"complete", retentionsAnswered.complete(),
			"unanswered", retentionsAnswered.unanswered(),
		)
	}
	inventoryComplete := inventory.complete()
	projection, err := r.projectPlacementInventory(ctx, reconcileProjectionInput{
		inventory:                 inventory,
		placementSnapshotRevision: placementSnapshotRevision,
		inventoryFence:            inventoryFence,
		inFlightAtSnapshot:        inFlightAtSnapshot,
	})
	if err != nil {
		return err
	}
	placementSyncOK := projection.syncOK
	placementRecords := projection.records
	sameSweepPlacementRevisions := projection.sameSweepPlacementRevisions
	ambiguousOwners := projection.ambiguousOwners

	// A fleet snapshot is authoritative only when both independent backend
	// inventories are complete and every positive observation/conflict has
	// crossed the durable placement boundary. Provision inventory alone is not
	// enough: retained data also pins ownership, and an unpersisted projection
	// cannot safely authorize lifecycle work or advance the success heartbeat.
	cycleComplete := inventoryComplete && (r.placementView == nil || placementSyncOK)
	if cycleComplete {
		metrics.ReconcilerSweepComplete.Set(1)
	} else {
		metrics.ReconcilerSweepComplete.Set(0)
	}
	var admissionBaseline placement.AdmissionBaseline
	if r.placementAuthority != nil {
		admissionBaseline = r.placementAuthority.CurrentAdmissionBaseline()
		r.placementSweepSeen.Store(admissionBaseline.Valid())
	}
	// A degraded sweep may place genuinely new work only on a backend that
	// answered both inventories. This is an immutable per-sweep hard boundary;
	// routing fallback is not allowed to escape it.
	eligibleBackends := make(map[string]struct{})
	for backendName, provisionsOK := range snapshot.answered {
		if provisionsOK && retentionsAnswered.heard(backendName) {
			eligibleBackends[backendName] = struct{}{}
		}
	}
	var recordlessScope placement.AdmissionScope
	if r.placementAuthority != nil && admissionBaseline.Valid() {
		recordlessScope, err = r.placementAuthority.ScopeAdmission(
			admissionBaseline, slices.Sorted(maps.Keys(eligibleBackends)),
		)
		if err != nil {
			return fmt.Errorf("scope recordless placement admission: %w", err)
		}
	}
	// Reuse one immutable placement snapshot across the dispatch loop. Lookup is
	// memory-backed, but doing it serially once also gives every worker the same
	// post-sync view. A worker refreshes only after its own successful attempt CAS,
	// because that mutation necessarily invalidates this snapshot for that lease.
	for leaseUUID, owners := range ambiguousOwners {
		slog.Error("reconcile: lease reported by multiple backends; preserving placement and deferring chain actions",
			"lease_uuid", leaseUUID,
			"backends", owners,
		)
	}

	// Snapshot of leases whose data lives on some backend (active or retained).
	// Built BEFORE allProvisions is mutated by orphan detection below — the pruner
	// needs the full pre-mutation set.
	backendLeases := make(map[string]struct{}, len(allProvisions)+len(allRetentions))
	for leaseUUID := range allProvisions {
		backendLeases[leaseUUID] = struct{}{}
	}
	for leaseUUID := range allRetentions {
		backendLeases[leaseUUID] = struct{}{}
	}

	// Check for cancellation before reconciliation loop
	if err := ctx.Err(); err != nil {
		return err
	}

	// 3. Reconcile each chain lease (with bounded concurrency)
	// First, collect all lease UUIDs to identify orphans after processing
	processedLeases := make(map[string]struct{}, len(chainLeases))
	for leaseUUID := range chainLeases {
		processedLeases[leaseUUID] = struct{}{}
	}

	var (
		provisioned  atomic.Int32
		acknowledged atomic.Int32
		anomalies    atomic.Int32
		leaseErrors  atomic.Int32
		deferred     atomic.Int32
	)

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(r.maxWorkers)

	for leaseUUID, lease := range chainLeases {
		provision, isProvisioned := allProvisions[leaseUUID]
		if owners, ambiguous := ambiguousOwners[leaseUUID]; ambiguous {
			deferred.Add(1)
			metrics.ReconcilerDeferredLeasesTotal.Inc()
			slog.Warn("reconcile: deferring lease with multiple positive backend owners",
				"lease_uuid", leaseUUID,
				"backends", owners,
			)
			continue
		}
		// A retention is positive evidence that this lease's data still lives on a
		// backend. Provision is not a restore operation, so it must not lay a fresh
		// volume over that data even when a complete sweep identifies one sole
		// retention owner. During an incomplete sweep an unanswered peer may also
		// retain another copy. Keep this gate lease-local so unrelated PENDING work
		// may still use the fully answering backend set.
		if retainedBackend, retained := allRetentions[leaseUUID]; retained {
			deferred.Add(1)
			metrics.ReconcilerDeferredLeasesTotal.Inc()
			slog.Warn("reconcile: deferring live lease positively reported as retained",
				"lease_uuid", leaseUUID,
				"retention_backend", retainedBackend,
			)
			continue
		}
		// ListProvisions may expose a backend's cached state after RefreshState
		// failed. Presence is useful conservative ownership evidence, but its
		// Ready/Failed/Provisioning status is not fresh enough to drive chain
		// transitions or reprovisioning.
		if isProvisioned && !snapshot.answered.heard(provision.BackendName) {
			deferred.Add(1)
			metrics.ReconcilerDeferredLeasesTotal.Inc()
			slog.Debug("reconcile: deferring lease whose reported status could not be refreshed",
				"lease_uuid", leaseUUID,
				"backend", provision.BackendName,
				"status", provision.Status,
			)
			continue
		}

		// Skip a lease whose owning backend did not report this sweep (ENG-356).
		//
		// This MUST skip only the work, never remove the lease from chainLeases
		// or processedLeases. Those two maps are read by three later passes, and
		// filtering them here would be silently destructive: a deferred lease
		// missing from processedLeases becomes an orphan candidate, and one
		// missing from chainLeases makes cleanupOrphanedPayloads delete a live
		// lease's payload — after which the NEXT sweep sees
		// errPayloadNotAvailable, classifies it as permanent, and closes a
		// healthy ACTIVE lease on chain. Skip the goroutine body; leave the maps
		// alone.
		g.Go(func() error {
			// Recover any panic inside this per-lease worker so ONE bad lease
			// doesn't crash fred. Log with full context, bump the
			// panic metric, count this lease as errored, and move on.
			// The next reconcile cycle will retry.
			defer func() {
				if rec := recover(); rec != nil {
					slog.Error("reconciler processLease panic — recovering to keep fred alive",
						"lease_uuid", leaseUUID,
						"panic", rec,
						"stack", string(debug.Stack()),
					)
					metrics.ReconcilerPanicsTotal.WithLabelValues("process_lease").Inc()
					leaseErrors.Add(1)
				}
			}()

			placementRecord := placementRecords[leaseUUID]
			deferForSnapshotBoundary := func(reason, operationID string) {
				deferred.Add(1)
				metrics.ReconcilerDeferredLeasesTotal.Inc()
				slog.Debug("reconcile: deferring lease whose operation crossed the fleet snapshot boundary",
					"lease_uuid", leaseUUID,
					"reason", reason,
					"operation_id", operationID,
					"placement_revision", placementRecord.Revision(),
					"inventory_revision", placementSnapshotRevision,
				)
			}
			if _, wasInFlight := inFlightAtSnapshot[leaseUUID]; wasInFlight {
				if lease.State == billingtypes.LEASE_STATE_PENDING && isProvisioned &&
					provision.Status == backend.ProvisionStatusReady {
					metrics.ReconcilerInflightSkipsTotal.Inc()
				}
				deferForSnapshotBoundary("in_flight_at_inventory_start", "")
				return nil
			}
			appliedRevision, appliedThisSweep := sameSweepPlacementRevisions[leaseUUID]
			if r.legacyPlacement != nil && placementRecord.State() != placement.StateAbsent &&
				placementRecord.Revision() > placementSnapshotRevision &&
				(!appliedThisSweep || appliedRevision != placementRecord.Revision()) {
				deferForSnapshotBoundary("placement_changed_during_inventory", "")
				return nil
			}
			placementCASRevision := placementSnapshotRevision
			if appliedThisSweep {
				// The store returned the exact revision committed by this inventory.
				// Passing only that identity lets an ACTIVE/Failed row create its
				// write-ahead attempt while any later placement mutation still wins.
				placementCASRevision = appliedRevision
			}

			// Attempt settlement can perform a bbolt write. Keep it inside the
			// bounded worker rather than serializing every lease in the dispatch
			// loop; its exact-revision CAS and the per-lease tracker snapshot make
			// settlements for distinct leases safe to run concurrently.
			_, absenceUntrustedForLease := r.placementAbsenceUntrusted[leaseUUID]
			if absenceUntrustedForLease {
				deferForSnapshotBoundary("placement_observation_excluded", "")
				return nil
			}
			attemptCleared := false
			if r.placementAuthority == nil {
				attemptCleared = r.resolvePlacementAttempt(
					leaseUUID, placementRecord, snapshot, placementSnapshotRevision,
					inFlightAtSnapshot, retentionsAnswered, retentionsReportedByBackend,
				)
			}
			if attemptCleared {
				placementRecord = r.placementFor(leaseUUID)
				if r.placementAuthority == nil {
					// The legacy ClearAttemptIfRevision path may delete an attempt-only
					// record, whose zero snapshot cannot expose the deletion revision.
					// Typed production uses the projection proof/tombstone directly.
					placementCASRevision = r.legacyPlacement.SnapshotRevision()
				}
			}
			allowRecordless := r.placementSweepSeen.Load() || cycleComplete
			if typedCoordination {
				allowRecordless = cycleComplete ||
					(placementSyncOK && admissionBaseline.Valid() &&
						lease.State == billingtypes.LEASE_STATE_PENDING && len(eligibleBackends) > 0)
			}
			absenceTrusted := allowRecordless || attemptCleared

			if placementRecord.State() == placement.StateConfirmed &&
				!snapshot.answered.configured(placementRecord.Backend) {
				deferred.Add(1)
				leaseErrors.Add(1)
				metrics.ReconcilerDeferredLeasesTotal.Inc()
				slog.Error("reconcile: refusing to provision, lease is placed on a backend the router does not know",
					"lease_uuid", leaseUUID,
					"placement_backend", placementRecord.Backend,
					"placement_state", placementRecord.State().String(),
				)
				return nil
			}
			if deferLease(
				snapshot, retentionsAnswered,
				isProvisioned, provision.BackendName, placementRecord, absenceTrusted,
			) {
				deferred.Add(1)
				metrics.ReconcilerDeferredLeasesTotal.Inc()
				slog.Debug("reconcile: deferring lease, owning backend did not report",
					"lease_uuid", leaseUUID,
					"placement_backend", placementRecord.Backend,
					"placement_attempt", placementRecord.Attempt,
					"placement_state", placementRecord.State().String(),
					"absence_trusted", absenceTrusted,
				)
				return nil
			}

			// Re-evaluate the two operation-owned facts immediately before lifecycle
			// action. An operation can start after the pre-sync snapshot above, or a
			// callback can settle after placementRecords was captured. Either change
			// makes this worker's chain/backend inputs stale for this lease. The typed
			// lease claim is the final process-local guard for every lifecycle action;
			// provisioning starts its operation under that exact claim, then consumes
			// the atomic placement projection proof before any backend side effect.
			currentPlacement := r.placementFor(leaseUUID)
			if currentPlacement.Revision() != placementRecord.Revision() ||
				currentPlacement.State() != placementRecord.State() {
				deferForSnapshotBoundary("placement_changed_after_sync", "")
				return nil
			}
			authority := reconcileActionAuthority{
				baseline:          admissionBaseline,
				recordlessScope:   recordlessScope,
				allowRecordless:   allowRecordless,
				placementRevision: placementCASRevision,
				trackerRevision:   trackerSnapshotRevision,
			}
			if !cycleComplete {
				authority.eligibleBackends = maps.Clone(eligibleBackends)
			}
			if currentPlacement.State() == placement.StateConfirmed {
				authority.ownedPlacement = currentPlacement.RecordRevision()
				authority.ownedBackend = currentPlacement.Backend
			}
			if typedCoordination {
				if current, inFlight := r.operations.Lookup(leaseUUID); inFlight {
					deferForSnapshotBoundary("operation_started_after_inventory", current.ID.String())
					return nil
				}
				claimResult := r.operations.TryClaimLease(leaseUUID, operationSnapshot)
				if !claimResult.Acquired() {
					reason := "operation_started_before_action_claim"
					if claimResult.Outcome() == operation.LeaseClaimSnapshotStale {
						reason = "operation_completed_after_inventory"
					}
					deferForSnapshotBoundary(reason, "")
					return nil
				}
				authority.leaseClaim = claimResult.Claim()
				defer r.operations.ReleaseLease(authority.leaseClaim)
			} else if r.tracker != nil {
				if _, inFlight := r.tracker.GetInFlight(leaseUUID); inFlight {
					deferForSnapshotBoundary("operation_started_after_inventory", "")
					return nil
				}
				claimed, snapshotStale := r.tracker.TryClaimLeaseActionIfNotNewer(
					leaseUUID, trackerSnapshotRevision,
				)
				if !claimed {
					reason := "operation_started_before_action_claim"
					if snapshotStale {
						reason = "operation_completed_after_inventory"
					}
					deferForSnapshotBoundary(reason, "")
					return nil
				}
				defer r.tracker.ReleaseLeaseAction(leaseUUID)
			}

			// The list queries are only inventory candidates. Re-read the exact
			// lease after acquiring its lifecycle claim so a close/reject/expiry
			// cannot cross the final chain-read -> backend-dispatch boundary. The
			// typed production path always has this authority; legacy test-double
			// construction retains its historical snapshot behavior.
			if typedCoordination {
				currentLease, err := r.chainClient.GetLease(gctx, leaseUUID)
				if err != nil {
					deferred.Add(1)
					leaseErrors.Add(1)
					metrics.ReconcilerDeferredLeasesTotal.Inc()
					slog.Error("reconcile: failed to re-read lease under lifecycle claim",
						"lease_uuid", leaseUUID,
						"error", err,
					)
					return nil
				}
				if currentLease == nil || currentLease.Uuid != leaseUUID ||
					(currentLease.State != billingtypes.LEASE_STATE_PENDING &&
						currentLease.State != billingtypes.LEASE_STATE_ACTIVE) {
					deferred.Add(1)
					metrics.ReconcilerDeferredLeasesTotal.Inc()
					slog.Debug("reconcile: lease changed after inventory; skipping lifecycle action",
						"lease_uuid", leaseUUID,
						"lease_state", leaseState(currentLease),
					)
					return nil
				}
				lease = *currentLease
				if !cycleComplete && lease.State != billingtypes.LEASE_STATE_PENDING {
					// Degraded recordless admission is PENDING-only. Recompute that
					// permission from the authoritative read under the lifecycle claim,
					// not from the earlier list snapshot.
					authority.allowRecordless = false
				}
			}

			r.processLease(gctx, leaseUUID, lease, provision, isProvisioned,
				authority, &provisioned,
				&acknowledged, &anomalies, &leaseErrors, &deferred)
			return nil // Don't fail fast - continue processing other leases
		})
	}

	// Wait for all lease processing to complete
	if err := g.Wait(); err != nil {
		return err
	}

	// Check for context cancellation after lease processing
	if ctx.Err() != nil {
		return ctx.Err()
	}

	// Remove processed leases from allProvisions to identify orphans
	for leaseUUID := range processedLeases {
		delete(allProvisions, leaseUUID)
	}

	// 4. Remaining provisions have no lease - check for orphans (with bounded concurrency)
	// Only deprovision orphans that belong to this provider to avoid
	// interfering with other providers sharing the same backend.
	//
	// This runs on every sweep, degraded or not (ENG-654). Every candidate is
	// positively attributed: it came from a backend that answered, and
	// processOrphan re-reads the lease from the chain and acts only on a
	// terminal state. A backend that did not answer contributes no candidates,
	// so partial fleet data can only ever UNDER-collect — which is why the old
	// fleet-wide gate protected nothing on the backend axis while pausing
	// cleanup for every healthy machine.
	var orphans atomic.Int32

	og, ogctx := errgroup.WithContext(ctx)
	og.SetLimit(r.maxWorkers)

	for leaseUUID, provision := range allProvisions {
		og.Go(func() error {
			// Recover any panic inside processOrphan. Same rationale as
			// the processLease recover above.
			defer func() {
				if rec := recover(); rec != nil {
					slog.Error("reconciler processOrphan panic — recovering to keep fred alive",
						"lease_uuid", leaseUUID,
						"panic", rec,
						"stack", string(debug.Stack()),
					)
					metrics.ReconcilerPanicsTotal.WithLabelValues("process_orphan").Inc()
					leaseErrors.Add(1)
				}
			}()
			r.processOrphan(
				ogctx, leaseUUID, provision, operationSnapshot, trackerSnapshotRevision,
				&orphans, &leaseErrors,
			)
			return nil // Don't fail fast - continue processing other orphans
		})
	}

	// Wait for all orphan processing to complete
	if err := og.Wait(); err != nil {
		return err
	}

	// Check for context cancellation after orphan processing
	if ctx.Err() != nil {
		return ctx.Err()
	}

	// Record action metrics
	provisionedCount := provisioned.Load()
	acknowledgedCount := acknowledged.Load()
	anomaliesCount := anomalies.Load()
	orphansCount := orphans.Load()
	leaseErrorCount := leaseErrors.Load()

	if provisionedCount > 0 {
		metrics.ReconciliationActions.WithLabelValues(metrics.ActionProvisioned).Add(float64(provisionedCount))
	}
	if acknowledgedCount > 0 {
		metrics.ReconciliationActions.WithLabelValues(metrics.ActionAcknowledged).Add(float64(acknowledgedCount))
	}
	if anomaliesCount > 0 {
		metrics.ReconciliationActions.WithLabelValues(metrics.ActionAnomaly).Add(float64(anomaliesCount))
	}
	if orphansCount > 0 {
		metrics.ReconciliationActions.WithLabelValues(metrics.ActionDeprovisioned).Add(float64(orphansCount))
	}
	if leaseErrorCount > 0 {
		metrics.ReconciliationActions.WithLabelValues(metrics.ActionLeaseError).Add(float64(leaseErrorCount))
	}

	// Record outcome. Exactly one value per sweep, most severe wins:
	// degraded (a whole backend was unreachable) outranks partial (individual
	// leases errored), and only a clean, complete sweep advances the
	// last-success timestamp — a degraded sweep did real work, but not for every
	// lease, so treating it as success would let the staleness alert go quiet
	// during precisely the outage it exists to catch.
	deferredCount := deferred.Load()
	switch {
	case !cycleComplete:
		metrics.ReconciliationTotal.WithLabelValues(metrics.OutcomeDegraded).Inc()
	case leaseErrorCount > 0:
		metrics.ReconciliationTotal.WithLabelValues(metrics.OutcomePartial).Inc()
	default:
		metrics.ReconciliationTotal.WithLabelValues(metrics.OutcomeSuccess).Inc()
		metrics.ReconcilerLastSuccessTimestamp.SetToCurrentTime()
	}

	// 5 & 6. The remaining passes DELETE durable state, and both run every sweep,
	// scoped to what this sweep can positively account for (ENG-654).
	//
	// Payload cleanup has no backend input at all — it compares the payload store
	// against the chain — so fleet completeness was never relevant to it; its one
	// hazard is chain-snapshot staleness, which it now settles per payload.
	//
	// Pruning placements is the more dangerous of the two: a record is the only
	// thing that will let a LATER sweep identify a lease's owner, so deleting one
	// during that backend's outage converts a transient failure into a
	// permanently unplaceable lease. That is a question about ONE backend, not
	// the fleet, so the pruner asks it per record — of both list endpoints,
	// which fail independently.
	orphanedPayloads := r.cleanupOrphanedPayloads(ctx, chainLeases)
	prunedPlacements := r.cleanupOrphanedPlacements(
		ctx, chainLeases, backendLeases, snapshot.answered, retentionsAnswered,
		startTime, placementSnapshotRevision, inFlightAtSnapshot,
		operationSnapshot, trackerSnapshotRevision,
	)

	logFunc := slog.Info
	if leaseErrorCount > 0 || !cycleComplete {
		logFunc = slog.Warn
	}
	logFunc("reconciliation complete",
		"provisioned", provisionedCount,
		"acknowledged", acknowledgedCount,
		"anomalies", anomaliesCount,
		"orphans", orphansCount,
		"errors", leaseErrorCount,
		"deferred", deferredCount,
		"sweep_complete", cycleComplete,
		"placement_sync_ok", r.placementView == nil || placementSyncOK,
		"placement_absence_trusted", r.placementSweepSeen.Load(),
		"orphaned_payloads_cleaned", orphanedPayloads,
		"orphaned_placements_pruned", prunedPlacements,
	)

	return nil
}

// startProvisioning initiates provisioning for a lease without a payload.
// Returns errLeaseAlreadyInFlight if the lease is already being provisioned by
// the event-driven path (this is not a real error, just a signal to skip).
func (r *Reconciler) startProvisioning(
	ctx context.Context,
	lease billingtypes.Lease,
	authority reconcileActionAuthority,
) error {
	return r.doStartProvisioning(ctx, lease, false, authority)
}

// startProvisioningWithPayload initiates provisioning for a lease that requires a payload.
// Returns errLeaseAlreadyInFlight if the lease is already being provisioned.
func (r *Reconciler) startProvisioningWithPayload(
	ctx context.Context,
	lease billingtypes.Lease,
	authority reconcileActionAuthority,
) error {
	return r.doStartProvisioning(ctx, lease, true, authority)
}

// doStartProvisioning is the common implementation for provisioning with or without payload.
func (r *Reconciler) doStartProvisioning(
	ctx context.Context,
	lease billingtypes.Lease,
	withPayload bool,
	authority reconcileActionAuthority,
) error {
	// Extract SKU for routing
	sku := ExtractRoutingSKU(&lease)

	// Route to appropriate backend, honoring existing placement for restored/placed leases (ENG-333)
	typedCoordination := r.operations != nil && r.placementAuthority != nil
	if typedCoordination && !authority.ownedPlacement.Valid() &&
		(!authority.allowRecordless || !authority.recordlessScope.Valid()) {
		return errPlacementSnapshotStale
	}
	var inFlightByBackend map[string]int
	if r.operations != nil {
		inFlightByBackend = r.operations.CountsByBackend()
	} else if r.tracker != nil {
		inFlightByBackend = r.tracker.InFlightCountsByBackend()
	}
	var backendClient backend.Backend
	var err error
	if typedCoordination && authority.ownedPlacement.Valid() {
		backendClient = r.backendRouter.GetBackendByName(authority.ownedBackend)
		if backendClient == nil {
			err = fmt.Errorf("%w: lease %s is placed on %q",
				ErrPlacementUnresolvable, lease.Uuid, authority.ownedBackend)
		}
	} else {
		backendClient, err = routeForProvisionHonoringPlacementAmong(
			ctx, r.backendRouter, r.placementView, lease.Uuid, sku,
			authority.eligibleBackends, inFlightByBackend,
		)
	}
	if err != nil {
		// ErrPlacementUnresolvable reaches handleProvisionError, whose default
		// branch treats it as transient: flag the sweep and retry next cycle.
		// It must never reach the reject/close branches — a paused or renamed
		// backend would then terminate healthy leases on chain (ENG-498).
		return err
	}
	if backendClient == nil {
		return fmt.Errorf("no backend available")
	}
	if typedCoordination && !authority.ownedPlacement.Valid() &&
		!authority.recordlessScope.Allows(backendClient.Name()) {
		return fmt.Errorf("%w: router selected %q outside the scoped healthy set",
			placement.ErrBackendOutsideAdmissionScope, backendClient.Name())
	}

	// Register the operation under the exact lease-action capability acquired
	// after inventory. That makes it impossible to lose the claim-to-operation
	// transition to an event-driven request for the same lease.
	items := ExtractLeaseItems(&lease)
	var (
		operationInitiation operation.Initiation
		legacyOperationID   operation.OperationID
	)
	if typedCoordination {
		if !authority.leaseClaim.Valid() || !authority.baseline.Valid() {
			return errTrackerSnapshotStale
		}
		initiated := r.operations.TryInitiateClaimed(authority.leaseClaim, operation.TrackSpec{
			LeaseUUID:     lease.Uuid,
			Tenant:        lease.Tenant,
			Items:         items,
			Backend:       backendClient.Name(),
			Kind:          operation.KindProvision,
			TokenRequired: true,
		})
		if !initiated.Started() {
			metrics.ReconciliationConflictsTotal.Inc()
			if initiated.Outcome() == operation.TrackSnapshotStale ||
				initiated.Outcome() == operation.TrackInvalid {
				return errTrackerSnapshotStale
			}
			return errLeaseAlreadyInFlight
		}
		operationInitiation = initiated.Capability()
	} else if r.tracker != nil {
		var tracked, snapshotStale bool
		legacyOperationID, tracked, snapshotStale = r.tracker.TryTrackInFlightWithOperationIDIfNotNewer(
			lease.Uuid, lease.Tenant, items, backendClient.Name(), authority.trackerRevision,
		)
		if !tracked {
			metrics.ReconciliationConflictsTotal.Inc()
			if snapshotStale {
				return errTrackerSnapshotStale
			}
			return errLeaseAlreadyInFlight
		}
	}
	untrack := func() {
		if typedCoordination {
			r.operations.AbortInitiation(operationInitiation)
		} else if r.tracker != nil {
			r.tracker.UntrackInFlightIfOperationID(lease.Uuid, legacyOperationID)
		}
	}

	callbackURL := BuildCallbackURL(r.callbackBaseURL)
	if typedCoordination {
		callbackURL, err = BuildCallbackURLForOperation(r.callbackBaseURL, operationInitiation.ID())
		if err != nil {
			untrack()
			return fmt.Errorf("build callback URL for lease %s: %w", lease.Uuid, err)
		}
	}

	// Build provision request.
	req := backend.ProvisionRequest{
		LeaseUUID:    lease.Uuid,
		Tenant:       lease.Tenant,
		ProviderUUID: r.providerUUID,
		Items:        items,
		CallbackURL:  callbackURL,
	}

	// Get the payload from the store WITHOUT removing it yet.
	// We only delete after Provision() succeeds to allow retries.
	// Only include PayloadHash when we have the actual payload - this ensures
	// backends never receive a hash without the corresponding data.
	if withPayload {
		payloadStore := r.payloadStore()
		if payloadStore == nil {
			if len(lease.MetaHash) > 0 {
				untrack()
				return fmt.Errorf("%w: lease %s", errPayloadNotAvailable, lease.Uuid)
			}
		} else {
			// Read the payload and its recorded hash from ONE snapshot. Two reads
			// would let a concurrent /update commit between them and hand this
			// attempt the old payload with the new hash — which fails verification
			// below and deletes the update that was just persisted. Both paths are
			// live at once for an ACTIVE lease whose provision has failed: the
			// reconciler re-provisions it while the backend still accepts /update
			// for it, and nothing serializes the two.
			recordedHash, getErr := []byte(nil), error(nil)
			req.Payload, recordedHash, getErr = payloadStore.GetWithHash(lease.Uuid)
			if getErr != nil {
				// Database error, or a recorded hash that is not a SHA-256 — do NOT
				// treat either as "payload missing". Abort this provision attempt so
				// a transient disk issue doesn't cause us to close an active lease.
				untrack()
				return fmt.Errorf("failed to read payload for lease %s: %w", lease.Uuid, getErr)
			}
			if req.Payload == nil && len(lease.MetaHash) > 0 {
				// Payload is required (lease has MetaHash) but not in the store.
				// This can happen if the payload DB was lost or fred restarted
				// without its data. We cannot re-provision without the manifest.
				untrack()
				return fmt.Errorf("%w: lease %s", errPayloadNotAvailable, lease.Uuid)
			}
			if req.Payload != nil && len(lease.MetaHash) > 0 {
				// Re-verify the payload before provisioning to catch corruption.
				//
				// Verify against the hash recorded when the payload was written, not
				// against the lease's on-chain MetaHash. MetaHash is set once at
				// lease creation and is immutable, so it names the manifest the
				// lease was CREATED with — while a tenant /update legitimately
				// replaces the stored manifest without changing it (ENG-619).
				// Checking an updated payload against MetaHash would read a
				// successful update as corruption, delete the payload, and then
				// close the ACTIVE lease on-chain via errPayloadNotAvailable.
				//
				// A payload with no recorded hash was written by a build that
				// predates the hash bucket; MetaHash remains the right reference for
				// it. ENG-643 makes the on-chain hash updatable and restores it as
				// the authoritative check, at which point the recorded hash becomes
				// a legacy fallback.
				expectedHash := recordedHash
				verifiedAgainst := "recorded_hash"
				if len(expectedHash) == 0 {
					expectedHash = lease.MetaHash
					verifiedAgainst = "meta_hash"
				}

				if err := payload.VerifyHash(req.Payload, expectedHash); err != nil {
					// Payload is corrupted - delete it and fail
					payloadStore.Delete(lease.Uuid)
					untrack()
					slog.Error("reconcile: payload hash mismatch - possible corruption",
						"lease_uuid", lease.Uuid,
						"verified_against", verifiedAgainst,
						"error", err,
					)
					return err
				}
				// The hash sent to the backend describes the payload actually being
				// sent, which after an update is no longer MetaHash.
				req.PayloadHash = hex.EncodeToString(expectedHash)
			}
		}
	}

	// Persist the target immediately before the external side effect. All local
	// payload reads and validation above intentionally happen first so a local
	// preflight failure never manufactures an attempt for a request not sent.
	var (
		attemptRevision uint64
		attemptToken    placement.AttemptToken
		attemptSet      = true
	)
	if typedCoordination {
		switch {
		case authority.ownedPlacement.Valid():
			attemptToken, attemptSet, err = r.placementAuthority.BeginOwnedAttempt(
				authority.baseline, authority.ownedPlacement,
				authority.ownedBackend, operationInitiation.ID(),
			)
		case authority.allowRecordless:
			attemptToken, attemptSet, err = r.placementAuthority.BeginNewAttempt(
				authority.recordlessScope,
				lease.Uuid, backendClient.Name(), operationInitiation.ID(),
			)
		default:
			attemptSet = false
		}
	} else if r.legacyPlacement != nil {
		attemptRevision, attemptSet, err = r.legacyPlacement.SetAttemptingIfNotNewer(
			lease.Uuid, backendClient.Name(), authority.placementRevision,
		)
	}
	if err != nil {
		untrack()
		if errors.Is(err, placement.ErrAttemptConflict) {
			return errLeaseAlreadyInFlight
		}
		return fmt.Errorf("record provision attempt for lease %s: %w", lease.Uuid, err)
	}
	if !attemptSet {
		untrack()
		return errPlacementSnapshotStale
	}
	if typedCoordination && !r.operations.BeginCall(operationInitiation) {
		refused, refuseErr := r.placementAuthority.RefuseAttempt(attemptToken)
		untrack()
		if refuseErr != nil {
			return fmt.Errorf("refuse unsent placement attempt for lease %s: %w",
				lease.Uuid, refuseErr)
		}
		if !refused {
			return errPlacementSnapshotStale
		}
		return errTrackerSnapshotStale
	}
	publishProvisionStartingBestEffort(r.startEvents, lease.Uuid, backendClient.Name())

	provisionErr := invokeBackendProvision(ctx, backendClient, req)
	outcome := classifyProvisionOutcome(provisionErr)
	if errors.Is(provisionErr, backend.ErrInsufficientResources) {
		metrics.BackendInsufficientResourcesTotal.WithLabelValues(backendClient.Name()).Inc()
	}
	if typedCoordination {
		var completion operation.InitiationCompletion
		if outcome == provisionOutcomeAccepted {
			completion = r.operations.Activate(operationInitiation)
		} else {
			completion = r.operations.AbortInitiation(operationInitiation)
		}
		switch completion {
		case operation.InitiationSettling, operation.InitiationFinished:
			// The exact callback owns every terminal side effect. In particular,
			// a synchronous error arriving after an inline Ready/Failed verdict
			// cannot clear its placement or trigger chain error handling.
			slog.Info("reconcile: inline provision callback superseded synchronous backend result",
				"lease_uuid", lease.Uuid,
				"backend", backendClient.Name(),
				"outcome", outcome,
			)
			return nil
		case operation.InitiationActivated:
			if outcome != provisionOutcomeAccepted {
				return errTrackerSnapshotStale
			}
		case operation.InitiationAborted:
			if outcome == provisionOutcomeAccepted {
				return errTrackerSnapshotStale
			}
		default:
			return errTrackerSnapshotStale
		}
	}

	var settleErr error
	if typedCoordination {
		var settled bool
		switch outcome {
		case provisionOutcomeAccepted:
			settled, settleErr = r.placementAuthority.ConfirmAttempt(attemptToken)
		case provisionOutcomeDefinitiveFailure:
			settled, settleErr = r.placementAuthority.RefuseAttempt(attemptToken)
		case provisionOutcomeAmbiguous:
			// Unknown backend outcomes deliberately retain the durable attempt.
			settled = true
		}
		if settleErr == nil && !settled && outcome == provisionOutcomeAccepted {
			// A backend may deliver its callback before the initiating HTTP request
			// returns. In that ordering the callback has already consumed the exact
			// operation and confirmed placement, so this process-local token is
			// expected to be stale and the positive durable fact wins.
			current := r.placementAuthority.Lookup(lease.Uuid)
			settled = current.State() == placement.StateConfirmed &&
				current.Backend == backendClient.Name() && current.Attempt == ""
		}
		if settleErr == nil && !settled {
			settleErr = errors.New("placement attempt is no longer current")
		}
	} else if r.legacyPlacement != nil {
		// Compatibility-only raw CAS for the unexported legacy test adapter.
		switch outcome {
		case provisionOutcomeAccepted:
			_, settleErr = r.legacyPlacement.ConfirmAttemptIfRevision(
				lease.Uuid, backendClient.Name(), attemptRevision,
			)
		case provisionOutcomeDefinitiveFailure:
			_, settleErr = r.legacyPlacement.ClearAttemptIfRevision(
				lease.Uuid, backendClient.Name(), attemptRevision,
			)
		case provisionOutcomeAmbiguous:
			// Preserve the durable attempt for a later authoritative inventory.
		default:
			settleErr = fmt.Errorf("unknown provision outcome %d", outcome)
		}
	}
	if settleErr != nil {
		slog.Warn("reconcile: failed to settle provision placement",
			"lease_uuid", lease.Uuid,
			"backend", backendClient.Name(),
			"outcome", outcome,
			"error", settleErr,
		)
	}

	switch outcome {
	case provisionOutcomeAccepted:
		if settleErr != nil {
			// The backend accepted the operation. Keep the tracker and durable
			// Attempt so neither event retries nor a degraded sweep can substitute
			// another backend; callback/SetBatch will repair confirmation.
			return fmt.Errorf("confirm accepted provision placement for lease %s: %w", lease.Uuid, settleErr)
		}
	case provisionOutcomeDefinitiveFailure, provisionOutcomeAmbiguous:
		// A definitive failure cleared Attempt when persistence succeeded. An
		// ambiguous result keeps it. Either way the durable record, rather than
		// the ephemeral tracker, gates the next call; releasing tracking lets the
		// reconciler consume a later authoritative inventory snapshot.
		if !typedCoordination {
			untrack()
		}
		return provisionErr
	}

	// Note: Payload is NOT deleted here. Cleanup happens later — when the
	// lease closes (HandleLeaseClosed) or when a PENDING-failure callback
	// rejects the lease and deletes the payload. Success and ACTIVE-failure
	// paths intentionally retain the payload so a subsequent re-provision
	// can reuse the same manifest. This also ensures the payload remains
	// available for retry if the backend fails or crashes before sending
	// a callback.

	if withPayload {
		slog.Info("reconcile: started provisioning with payload",
			"lease_uuid", lease.Uuid,
			"tenant", lease.Tenant,
			"sku", sku,
			"backend", backendClient.Name(),
			"payload_size", len(req.Payload),
		)
	} else {
		slog.Info("reconcile: started provisioning",
			"lease_uuid", lease.Uuid,
			"tenant", lease.Tenant,
			"sku", sku,
			"backend", backendClient.Name(),
		)
	}

	return nil
}

// acknowledgeLease acknowledges a lease via the batcher for parallel signing.
func (r *Reconciler) acknowledgeLease(ctx context.Context, leaseUUID string) error {
	acknowledged, txHash, err := r.acknowledger.Acknowledge(ctx, leaseUUID)
	if err != nil {
		return err
	}

	slog.Info("reconcile: acknowledged lease",
		"lease_uuid", leaseUUID,
		"acknowledged", acknowledged,
		"tx_hash", txHash,
	)

	return nil
}

// rejectLease rejects a PENDING lease on chain with a reason.
func (r *Reconciler) rejectLease(ctx context.Context, leaseUUID, reason string) error {
	rejected, txHashes, err := r.chainClient.RejectLeases(ctx, []string{leaseUUID}, truncateRejectReason(reason))
	if err != nil {
		return err
	}

	r.cleanupTerminalLease(leaseUUID)

	// The provisioning path clears its own matching Attempt before asking us to
	// reject. Do not unconditionally delete here: an event-driven writer may
	// have installed a newer attempt between the backend refusal and this chain
	// transaction. Any conservative residue is removed by the revision-gated
	// placement pruner once both backend inventories and chain terminality agree.

	slog.Info("reconcile: rejected lease",
		"lease_uuid", leaseUUID,
		"rejected", rejected,
		"tx_hashes", txHashes,
		"reason", reason,
	)

	return nil
}

// closeLease closes an ACTIVE lease on chain with a reason.
func (r *Reconciler) closeLease(ctx context.Context, leaseUUID, reason string) error {
	closed, txHashes, err := r.chainClient.CloseLeases(ctx, []string{leaseUUID}, reason)
	if err != nil {
		return err
	}

	r.cleanupTerminalLease(leaseUUID)

	slog.Info("reconcile: closed lease",
		"lease_uuid", leaseUUID,
		"closed", closed,
		"tx_hashes", txHashes,
		"reason", reason,
	)

	return nil
}

// cleanupTerminalLease removes the stored payload for a lease that has reached
// a terminal state (rejected or closed).
//
// Placement is intentionally NOT deleted here (ENG-333): if the backend
// retained the volumes on close, the placement record must survive so that a
// subsequent restore request can resolve the correct backend. The gated pruner
// (cleanupOrphanedPlacements) is the sole owner of placement deletion for
// closed leases — it keeps a still-retained lease and prunes a
// genuinely-gone one once every gate is satisfied.
//
// A PENDING refusal is normally already absent because the attempt lifecycle
// cleared its matching write-ahead record. Any residue is left to the
// revision-gated pruner rather than racing a newer writer here.
func (r *Reconciler) cleanupTerminalLease(leaseUUID string) {
	if ps := r.payloadStore(); ps != nil {
		ps.Delete(leaseUUID)
	}
}

// answeredSet records, per configured backend name, whether it answered one
// particular list endpoint this sweep. A backend that errored, panicked or timed
// out is present with the value false; absent from the map means not configured
// at all.
//
// It is a named type because "did this backend answer?" is now asked per item
// rather than fleet-wide (ENG-654), and asked of two independent endpoints
// (/provisions and /retentions) that fail independently.
type answeredSet map[string]bool

// ambiguousReportedOwners returns leases positively reported by more than one
// backend across any supplied inventory endpoint. Such evidence is useful for
// preventing duplicate provisioning, but no individual report is authoritative
// enough to choose affinity or drive a chain transition.
func ambiguousReportedOwners(reports ...map[string]map[string]struct{}) map[string][]string {
	ownersByLease := make(map[string]map[string]struct{})
	for _, report := range reports {
		for backendName, leases := range report {
			for leaseUUID := range leases {
				owners := ownersByLease[leaseUUID]
				if owners == nil {
					owners = make(map[string]struct{})
					ownersByLease[leaseUUID] = owners
				}
				owners[backendName] = struct{}{}
			}
		}
	}

	ambiguous := make(map[string][]string)
	for leaseUUID, owners := range ownersByLease {
		if len(owners) < 2 {
			continue
		}
		names := slices.Sorted(maps.Keys(owners))
		ambiguous[leaseUUID] = names
	}
	return ambiguous
}

// updatePlacementAmbiguities merges this sweep's conflicts into the
// process-local quarantine. Only a complete view of both inventory endpoints
// can prove an older conflict has resolved. ReconcileAll is serialized by
// reconciling, so this map needs no independent lock.
func (r *Reconciler) updatePlacementAmbiguities(current map[string][]string, complete bool) map[string][]string {
	if r.ambiguousPlacements == nil {
		r.ambiguousPlacements = make(map[string][]string)
	}
	if complete {
		for leaseUUID := range r.ambiguousPlacements {
			if _, stillAmbiguous := current[leaseUUID]; !stillAmbiguous {
				delete(r.ambiguousPlacements, leaseUUID)
			}
		}
	}
	for leaseUUID, owners := range current {
		r.ambiguousPlacements[leaseUUID] = slices.Clone(owners)
	}

	out := make(map[string][]string, len(r.ambiguousPlacements))
	for leaseUUID, owners := range r.ambiguousPlacements {
		out[leaseUUID] = slices.Clone(owners)
	}
	return out
}

// heard reports whether this sweep has a usable report from the named backend.
//
// The empty name and an unconfigured backend both answer false, deliberately:
// absence from the map is not "no objection", it is "fred cannot account for
// this item" — the same rule deferLease applies on the read path and ENG-635
// applies on the write path.
func (a answeredSet) heard(name string) bool {
	return name != "" && a[name]
}

// configured reports whether the named backend belongs to this process's
// current router, independently of whether it answered. That distinction makes
// a durable pin to a removed backend an actionable configuration error while a
// transient outage remains an ordinary deferred lease.
func (a answeredSet) configured(name string) bool {
	if name == "" {
		return false
	}
	_, ok := a[name]
	return ok
}

// complete reports whether every configured backend answered.
func (a answeredSet) complete() bool {
	for _, ok := range a {
		if !ok {
			return false
		}
	}
	return true
}

// unanswered lists the backends that did not report, for logging.
func (a answeredSet) unanswered() []string {
	var out []string
	for name, ok := range a {
		if !ok {
			out = append(out, name)
		}
	}
	slices.Sort(out)
	return out
}

// fleetSnapshot is one sweep's view of the fleet: the provisions reported by
// the backends that ANSWERED, which backends those were, and whether that is
// all of them.
//
// It is a named type rather than a tuple because its two views of the same fact
// answer different questions: `complete` is the fleet-wide one, consumed by the
// per-lease deferral guard, the retention-derived placement backfill and the
// sweep-outcome metric, while `answered` is the per-backend one the placement
// pruner asks of a single record (ENG-654).
type fleetSnapshot struct {
	// provisions is the union over answering backends, keyed by lease UUID.
	provisions map[string]backend.ProvisionInfo
	// reportedByBackend retains the per-backend membership that the union above
	// intentionally flattens. Placement attempts use it to distinguish "the
	// attempted backend answered without this lease" from "some other backend
	// reported the same lease".
	reportedByBackend map[string]map[string]struct{}
	// answered reports, per configured backend name, whether it returned its
	// provisions this sweep.
	answered answeredSet
	// complete is true when every configured backend answered. It is the
	// authority for "absence is evidence": only on a complete sweep does a
	// lease's absence from provisions prove it is not provisioned anywhere.
	complete bool
}

// fetchFleetSnapshot retrieves provisions from all backends in parallel and
// reports which of them answered.
//
// It never fails the sweep. A backend that cannot be reached is recorded as
// unanswered and its leases are deferred by the caller (ENG-356); previously
// any single failure aborted reconciliation for the entire fleet, so one quiet
// backend froze self-healing for every other backend's leases — degrading as
// p^n, i.e. getting worse as the fleet grows.
//
// What partial data can and cannot do is worth stating precisely, because the
// original rationale here had it half backwards:
//
//   - It CANNOT manufacture orphans. Orphan candidates are provisions minus the
//     complete set of chain leases, so a backend that did not answer
//     contributes zero candidates. Partial data only ever under-collects.
//   - It CAN make a live lease look unprovisioned, which is the real hazard:
//     the ACTIVE-but-not-provisioned row would re-provision it onto a healthy
//     peer, laying an empty volume over live tenant data. That is exactly what
//     the caller's deferral guard prevents.
func (r *Reconciler) fetchFleetSnapshot(ctx context.Context) fleetSnapshot {
	backends := r.backendRouter.Backends()

	g, gctx := errgroup.WithContext(ctx)
	if len(backends) > 0 {
		g.SetLimit(len(backends)) // Query all backends concurrently
	}

	var mu sync.Mutex
	snap := fleetSnapshot{
		provisions:        make(map[string]backend.ProvisionInfo),
		reportedByBackend: make(map[string]map[string]struct{}, len(backends)),
		answered:          make(answeredSet, len(backends)),
		complete:          true,
	}

	for _, b := range backends {
		g.Go(func() (goErr error) {
			// Recover any panic from backend.RefreshState / ListProvisions (or
			// the HTTP/JSON path implementing them). We do NOT surface it as an
			// errgroup error: returning non-nil would trip errgroup's fail-fast
			// and cancel sibling fetches via gctx, turning one bad backend into
			// a fleet-wide failure — the very coupling this function exists to
			// remove. A panicking backend is simply one that did not answer.
			defer func() {
				if rec := recover(); rec != nil {
					slog.Error("reconciler fetch panic — recovering to keep fred alive",
						"backend", b.Name(),
						"panic", rec,
						"stack", string(debug.Stack()),
					)
					metrics.ReconcilerPanicsTotal.WithLabelValues("fetch_provisions").Inc()
					mu.Lock()
					snap.markUnanswered(b.Name())
					mu.Unlock()
					metrics.ReconcilerBackendFetchTotal.WithLabelValues(b.Name(), metrics.FetchOutcomePanic).Inc()
					goErr = nil // Don't cancel siblings via errgroup.
				}
			}()
			// Ensure backend state is fresh before reading provisions. A failed refresh
			// does not discard stale positive entries, but it does make this backend
			// non-authoritative for negative evidence: stale absence cannot clear an
			// attempt or arm the complete-sweep trust latch.
			refreshErr := b.RefreshState(gctx)
			if refreshErr != nil {
				slog.Warn("failed to refresh backend state",
					"backend", b.Name(), "error", refreshErr,
				)
				// Continue — stale positive state is still conservative and useful.
			}

			provisions, err := b.ListProvisions(gctx)
			if err != nil {
				slog.Error("failed to list provisions from backend",
					"backend", b.Name(),
					"error", err,
				)
				mu.Lock()
				snap.markUnanswered(b.Name())
				mu.Unlock()
				// An open circuit breaker is reported distinctly: it means fred
				// short-circuited without dialing, which reads very differently
				// in an incident from a backend that was actually contacted.
				outcome := metrics.FetchOutcomeError
				if errors.Is(err, backend.ErrCircuitOpen) {
					outcome = metrics.FetchOutcomeCircuitOpen
				}
				metrics.ReconcilerBackendFetchTotal.WithLabelValues(b.Name(), outcome).Inc()
				return nil // Don't cancel other backends
			}

			mu.Lock()
			if refreshErr == nil {
				snap.answered[b.Name()] = true
			} else {
				snap.markUnanswered(b.Name())
			}
			reported := make(map[string]struct{}, len(provisions))
			for _, p := range provisions {
				p.BackendName = b.Name()
				snap.provisions[p.LeaseUUID] = p
				reported[p.LeaseUUID] = struct{}{}
			}
			snap.reportedByBackend[b.Name()] = reported
			mu.Unlock()
			fetchOutcome := metrics.FetchOutcomeOK
			if refreshErr != nil {
				fetchOutcome = metrics.FetchOutcomeError
				if errors.Is(refreshErr, backend.ErrCircuitOpen) {
					fetchOutcome = metrics.FetchOutcomeCircuitOpen
				}
			}
			metrics.ReconcilerBackendFetchTotal.WithLabelValues(b.Name(), fetchOutcome).Inc()

			slog.Debug("fetched backend provisions",
				"backend", b.Name(),
				"count", len(provisions),
			)
			return nil
		})
	}

	_ = g.Wait() // closures never return non-nil; outcomes are recorded in snap

	return snap
}

// placementFor returns the complete placement record. A deployment without a
// placement store receives the zero (Absent) record and never arms the
// complete-sweep latch, so unevidenced leases remain conservative whenever a
// fleet snapshot is incomplete.
func (r *Reconciler) placementFor(leaseUUID string) placement.Placement {
	if r.placementView == nil {
		return placement.Placement{}
	}
	return r.placementView.Lookup(leaseUUID)
}

// resolvePlacementAttempt is retained only for the raw compatibility adapter.
// Inventory absence deliberately never settles an outbound attempt: it is not
// causally ordered after the remote effect, so a delayed backend request could
// still commit after both list endpoints answered without the lease. Typed
// production confirms an attempt from positive inventory or clears it through
// an exact synchronous-refusal capability.
func (r *Reconciler) resolvePlacementAttempt(
	leaseUUID string,
	p placement.Placement,
	snap fleetSnapshot,
	snapshotRevision uint64,
	inFlightAtSnapshot map[string]struct{},
	retentionsAnswered answeredSet,
	retentionsReportedByBackend map[string]map[string]struct{},
) bool {
	_ = r
	_ = leaseUUID
	_ = p
	_ = snap
	_ = snapshotRevision
	_ = inFlightAtSnapshot
	_ = retentionsAnswered
	_ = retentionsReportedByBackend
	return false
}

// markUnanswered records that a backend did not report this sweep. Callers must
// hold the snapshot's mutex.
func (s *fleetSnapshot) markUnanswered(name string) {
	s.answered[name] = false
	s.complete = false
}

// unansweredBackends lists the backends that did not report, for logging.
func (s fleetSnapshot) unansweredBackends() []string {
	return s.answered.unanswered()
}

// deferLease reports whether this sweep must skip a lease because it could not
// positively identify which backend owns it.
//
// This is a pure function of the sweep's evidence, deliberately: it is the one
// safety decision in the reconciler that must be exhaustively testable without
// standing up a fleet, and keeping it free of receiver state means a future
// per-backend reconciler could reuse it unchanged for the leases no single
// backend loop owns.
//
// The rule is that fred acts only on unambiguous positive evidence of ownership.
// A conflict/unusable record and an unresolved mismatched attempt defer even if
// one backend reports the lease. Otherwise, a reported provision proceeds. A
// confirmed placement proceeds only when its own backend answered. When the
// lease is missing from provision inventory, that negative evidence is
// actionable only if the exact owner also answered retention inventory: an
// unreadable retention store may be hiding the lease's surviving data.
// A genuinely absent record proceeds on a complete sweep, after this process
// has previously completed a durable placement sync, or after this sweep
// authoritatively cleared that lease's sole attempt.
//
// Deferral is never destructive — it skips work rather than doing different
// work — so the cost of over-deferring is latency, while the cost of
// under-deferring is an empty volume laid over live tenant data.
func deferLease(
	snap fleetSnapshot,
	retentionsAnswered answeredSet,
	isProvisioned bool,
	reportedBackend string,
	p placement.Placement,
	absenceTrusted bool,
) bool {
	if p.State() == placement.StateUnusable {
		return true
	}
	// An unresolved attempt is an execution gate, not an affinity pin. It must
	// be settled by positive inventory or authoritative absence before another
	// backend call can be made.
	if p.Attempt != "" {
		return true
	}
	if p.Backend != "" {
		if isProvisioned && reportedBackend != p.Backend {
			return true
		}
		if !snap.answered.heard(p.Backend) {
			return true
		}
		return !isProvisioned && !retentionsAnswered.heard(p.Backend)
	}
	if isProvisioned {
		return false
	}
	return !snap.complete && !absenceTrusted
}

// leaseLiveness is what the chain says about one lease right now, as opposed to
// what this sweep's snapshot said about it several seconds ago.
type leaseLiveness int

const (
	// leaseUnknown means fred could not establish the lease's state: the query
	// failed, the chain has no record of it, or it carries the zero state.
	leaseUnknown leaseLiveness = iota
	// leaseLive means PENDING or ACTIVE — the lease is a going concern and the
	// main reconcile loop owns it.
	leaseLive
	// leaseTerminal means CLOSED, REJECTED or EXPIRED. It is the ONLY verdict
	// that authorizes destroying the lease's state, and it is monotone: the
	// chain never moves a lease back out of a terminal state.
	leaseTerminal
)

// classifyLease maps a GetLease result to the three-state rule the destructive
// passes act on, and names the metric reason for every non-terminal verdict.
//
// It is a pure function for the same reason deferLease is: it is a safety
// decision that must be exhaustively testable without standing up a fleet.
//
// The rule is that fred destroys state only on positive evidence that the lease
// is finished — never on absence, and never on a failed read:
//
//   - An error is not absence. The chain was unreachable or slow; try again
//     next sweep.
//   - A nil lease is not "closed". x/billing never deletes a lease — CloseLease
//     sets State in place — so the chain having no record means it never knew
//     this lease: a phantom provision, a wrong or reset chain, or an RPC node
//     behind the head. None of those are a license to delete a tenant's data,
//     and reading them as one is how "both list queries returned empty" turns
//     into deprovisioning the entire fleet.
//   - PENDING/ACTIVE means the sweep's chain snapshot was simply stale: the two
//     list queries are not atomic, so a lease created between them is invisible
//     to both while the event path may already be provisioning it.
//   - Any other state — the zero UNSPECIFIED, or a value this build has never
//     heard of — is unknown. Terminality is an ALLOWLIST, never "not one of the
//     live ones": LeaseState is a plain int32 and the generated unmarshaller
//     decodes it as a raw varint (`m.State |= LeaseState(b&0x7F) << shift`) with
//     no validation, so a state added to the chain after this binary shipped
//     arrives as an unrecognized number. Under a denylist it would read as
//     terminal, and fred would deprovision live leases across the fleet the
//     moment the chain gained a state it does not know.
//
// The asymmetry is the same one deferLease documents. Skipping costs a cycle of
// cleanup latency; acting on a lease that is not finished costs a tenant their
// workload.
func classifyLease(lease *billingtypes.Lease, err error) (leaseLiveness, string) {
	switch {
	case err != nil:
		return leaseUnknown, metrics.CleanupSkipChainError
	case lease == nil:
		return leaseUnknown, metrics.CleanupSkipChainUnknown
	}

	switch lease.State {
	case billingtypes.LEASE_STATE_PENDING, billingtypes.LEASE_STATE_ACTIVE:
		return leaseLive, metrics.CleanupSkipChainLive
	case billingtypes.LEASE_STATE_CLOSED, billingtypes.LEASE_STATE_REJECTED, billingtypes.LEASE_STATE_EXPIRED:
		return leaseTerminal, ""
	default:
		return leaseUnknown, metrics.CleanupSkipChainUnknownState
	}
}

// leaseState renders a lease's state for a log line. LeaseState.String() falls
// back to the raw number for a value this build has no name for, which is
// exactly the case worth printing.
func leaseState(lease *billingtypes.Lease) string {
	if lease == nil {
		return "<no lease>"
	}
	return lease.State.String()
}

// queryLeaseLiveness performs the bounded point read shared by destructive
// cleanup confirmation and conservative placement-marker retirement. Keeping
// the query separate from either caller's instrumentation prevents bookkeeping
// checks from masquerading as withheld destructive cleanup.
func (r *Reconciler) queryLeaseLiveness(
	ctx context.Context,
	leaseUUID string,
) (leaseLiveness, string, *billingtypes.Lease, error) {
	qctx, cancel := context.WithTimeout(ctx, chainConfirmTimeout)
	defer cancel()

	lease, err := r.chainClient.GetLease(qctx, leaseUUID)
	liveness, reason := classifyLease(lease, err)
	return liveness, reason, lease, err
}

// placementMarkerLeaseTerminal reports whether an in-memory placement-absence
// marker may be retired. Keeping a marker only preserves a conservative routing
// gate; it does not withhold or perform destructive cleanup. Consequently an
// uncertain verdict is debug context only: it must not increment
// cleanup_skips_total or tell an operator to deprovision a phantom resource.
func (r *Reconciler) placementMarkerLeaseTerminal(ctx context.Context, leaseUUID string) bool {
	liveness, reason, lease, err := r.queryLeaseLiveness(ctx, leaseUUID)
	if liveness == leaseTerminal {
		return true
	}

	slog.Debug("reconcile: retaining placement absence marker; chain does not confirm the lease is finished",
		"lease_uuid", leaseUUID,
		"reason", reason,
		"lease_state", leaseState(lease),
		"error", err,
	)
	return false
}

// confirmTerminal re-reads one lease from the chain and reports whether it is
// positively finished. A false return has already counted itself and logged;
// the caller just skips the lease.
//
// This runs on every candidate, not only on degraded sweeps: the staleness it
// guards against comes from the sweep's own non-atomic chain queries, and is
// identical on a sweep that saw every backend.
func (r *Reconciler) confirmTerminal(ctx context.Context, pass, leaseUUID string) bool {
	liveness, reason, lease, err := r.queryLeaseLiveness(ctx, leaseUUID)
	if liveness == leaseTerminal {
		return true
	}

	metrics.ReconcilerCleanupSkipsTotal.WithLabelValues(pass, reason).Inc()
	switch reason {
	case metrics.CleanupSkipChainUnknown:
		// Not self-healing, unlike chain_live and chain_error: fred will decline
		// this candidate on every future sweep as well, so say so once per sweep
		// at a level an operator sees.
		slog.Warn("reconcile: chain has no record of this lease — refusing to destroy its state, MANUAL CLEANUP MAY BE REQUIRED",
			"lease_uuid", leaseUUID,
			"pass", pass,
			"error", err,
		)
	case metrics.CleanupSkipChainUnknownState:
		// Also not self-healing, but the remediation is the opposite one, so it
		// must not share the message above: the chain knows this lease perfectly
		// well and fred cannot read its state. Upgrading fred is the fix, not
		// hunting a phantom provision or a misconfigured endpoint.
		slog.Warn("reconcile: lease is in a state this build does not recognize — refusing to destroy its state, fred may be older than the chain",
			"lease_uuid", leaseUUID,
			"pass", pass,
			"lease_state", leaseState(lease),
		)
	default:
		slog.Info("reconcile: skipping cleanup, chain does not confirm the lease is finished",
			"lease_uuid", leaseUUID,
			"pass", pass,
			"reason", reason,
			"error", err,
		)
	}
	return false
}

// fetchAllRetentions queries every backend's retained leases in parallel,
// returning leaseUUID→backendName and, per backend, whether it answered.
//
// The answered set gates placement pruning per record (ENG-654): a record whose
// own backend did not report its retentions must not be pruned, because a
// transient outage on that machine would otherwise look like "the data is gone".
// A backend that DID answer accounts for its own records, so its silence about
// one of them is real evidence. Retention positives from a partial sweep remain
// useful conservatively, but placement backfill waits for a complete snapshot.
func (r *Reconciler) fetchAllRetentions(ctx context.Context) (map[string]string, answeredSet, map[string]map[string]struct{}) {
	backends := r.backendRouter.Backends()

	var mu sync.Mutex
	out := make(map[string]string)
	answered := make(answeredSet, len(backends))
	reportedByBackend := make(map[string]map[string]struct{}, len(backends))

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(len(backends))
	for _, b := range backends {
		g.Go(func() (goErr error) {
			defer func() {
				if rec := recover(); rec != nil {
					slog.Error("reconciler retentions fetch panic — recovering",
						"backend", b.Name(), "panic", rec, "stack", string(debug.Stack()))
					metrics.ReconcilerPanicsTotal.WithLabelValues("fetch_retentions").Inc()
					mu.Lock()
					answered[b.Name()] = false
					mu.Unlock()
					goErr = nil
				}
			}()
			// No RefreshState needed: ListRetentions reads the backend's persistent
			// retention store (always current), unlike ListProvisions' in-memory state.
			retentions, err := b.ListRetentions(gctx)
			if err != nil {
				slog.Warn("failed to list retentions from backend",
					"backend", b.Name(), "error", err)
				mu.Lock()
				answered[b.Name()] = false
				mu.Unlock()
				return nil // collect from other backends; don't cancel
			}
			reported := make(map[string]struct{}, len(retentions))
			for _, ret := range retentions {
				reported[ret.LeaseUUID] = struct{}{}
			}
			mu.Lock()
			answered[b.Name()] = true
			reportedByBackend[b.Name()] = reported
			for _, ret := range retentions {
				out[ret.LeaseUUID] = b.Name()
			}
			mu.Unlock()
			slog.Debug("fetched backend retentions", "backend", b.Name(), "count", len(retentions))
			return nil
		})
	}
	_ = g.Wait() // closures never return non-nil; outcomes are recorded in answered

	return out, answered, reportedByBackend
}

// handleProvisionError handles errors from provisioning attempts during reconciliation.
// It determines the appropriate action based on error type and lease state:
//   - errLeaseAlreadyInFlight: skip (not a real error)
//   - backend.ErrAlreadyProvisioned: transient (an unvalidated HTTP 409 is ambiguous)
//   - errPayloadNotAvailable: reject (PENDING) or close (ACTIVE) the lease
//   - backend.ErrValidation: reject (PENDING) or close (ACTIVE) the lease
//   - backend.ErrMalformedErrorBody: transient (backend answered off-contract) — flag for retry, never terminate
//   - ErrPlacementUnresolvable: transient (backend absent from config) — flag for retry, never terminate
//   - backend.ErrCircuitOpen: transient (breaker auto-recovers) — flag for retry, never terminate
//   - other errors: log and flag for retry next cycle
func (r *Reconciler) handleProvisionError(ctx context.Context, err error, leaseUUID string, lease billingtypes.Lease, hadError *bool) {
	if errors.Is(err, errLeaseAlreadyInFlight) {
		slog.Debug("reconcile: lease already in-flight, skipping", "lease_uuid", leaseUUID)
		return
	}
	if errors.Is(err, backend.ErrAlreadyProvisioned) {
		// HTTPClient maps an unvalidated 409 to this sentinel, so it is not durable
		// ownership proof. The write-ahead Attempt prevents substitution while a
		// later complete inventory resolves whether the backend actually owns it.
		slog.Warn("reconcile: backend returned ambiguous already-provisioned response, awaiting inventory",
			"lease_uuid", leaseUUID,
			"tenant", lease.Tenant,
			"error", err,
		)
		*hadError = true
		return
	}
	if errors.Is(err, ErrPlacementUnresolvable) {
		// The lease is pinned to a backend the router does not know, so fred
		// refuses to provision it anywhere (ENG-635). Handled explicitly rather
		// than left to the transient default below: this is the unattended path,
		// so it needs its own greppable log line, and stating the classification
		// here makes "never terminate for this" a property of the code rather
		// than an accident of ordering. A backend is typically absent because it
		// was paused, renamed or is mid-redeploy — closing paying leases for that
		// would turn a maintenance window into permanent data loss (ENG-498).
		p := r.placementFor(leaseUUID)
		slog.Error("reconcile: refusing to provision, lease is placed on a backend the router does not know",
			"lease_uuid", leaseUUID,
			"tenant", lease.Tenant,
			"placement_backend", p.Backend,
			"error", err,
		)
		*hadError = true
		return
	}
	if errors.Is(err, backend.ErrMalformedErrorBody) {
		// The backend rejected the request with a body fred could not parse, so
		// fred does not know WHY — and cannot know the backend even authored it
		// (an intermediary emitting its own 4xx looks identical). Stated as its
		// own branch rather than left to the transient default below so that
		// "never terminate on an unparseable answer" is a property of the code:
		// the pre-ENG-620 client wrapped every 400 in ErrValidation, which lands
		// in the permanent switch and CLOSES an ACTIVE lease on-chain.
		//
		// Operator-introduced, not tenant-reachable (ENG-739): docker-backend
		// routes every 4xx through its ErrorResponse writer, and the ENG-356
		// snapshot gate already defers a backend whose GET /provisions did not
		// answer, so the reachable variant is a selective intermediary failure.
		// Self-limiting from the other side too: the client counts this toward
		// the circuit breaker, so a persistently off-contract backend degrades
		// into the ErrCircuitOpen arm below rather than looping here forever.
		slog.Error("reconcile: backend returned an unusable error body, will retry next cycle",
			"lease_uuid", leaseUUID,
			"tenant", lease.Tenant,
			"error", err,
		)
		*hadError = true
		return
	}
	if errors.Is(err, backend.ErrCircuitOpen) {
		// The backend circuit breaker is open — a TRANSIENT condition that
		// auto-recovers once the breaker half-opens (gobreaker CBTimeout). A
		// brief backend blip must never permanently reject/close an otherwise
		// recoverable lease on-chain; flag the cycle for retry instead (ENG-498).
		slog.Warn("reconcile: backend circuit open, will retry next cycle",
			"lease_uuid", leaseUUID,
			"tenant", lease.Tenant,
			"error", err,
		)
		*hadError = true
		return
	}

	// Determine the termination reason for permanent errors
	var reason string
	switch {
	case errors.Is(err, errPayloadNotAvailable):
		reason = "payload not available for re-provisioning"
	case errors.Is(err, backend.ErrValidation):
		reason = validationErrorToRejectReason(err)
	default:
		// Transient error — log and retry next cycle
		slog.Error("reconcile: provisioning failed",
			"lease_uuid", leaseUUID,
			"tenant", lease.Tenant,
			"error", err,
		)
		*hadError = true
		return
	}

	// Permanent error — terminate the lease
	isPending := lease.State == billingtypes.LEASE_STATE_PENDING
	if isPending {
		slog.Warn("reconcile: permanent provisioning error, rejecting pending lease",
			"lease_uuid", leaseUUID,
			"tenant", lease.Tenant,
			"reason", reason,
			"error", err,
		)
		if rejectErr := r.rejectLease(ctx, leaseUUID, reason); rejectErr != nil {
			slog.Error("reconcile: failed to reject lease",
				"lease_uuid", leaseUUID,
				"error", rejectErr,
			)
			*hadError = true
		}
	} else {
		slog.Error("reconcile: permanent provisioning error, closing active lease",
			"lease_uuid", leaseUUID,
			"tenant", lease.Tenant,
			"reason", reason,
			"error", err,
		)
		if closeErr := r.closeLease(ctx, leaseUUID, reason); closeErr != nil {
			slog.Error("reconcile: failed to close lease",
				"lease_uuid", leaseUUID,
				"error", closeErr,
			)
			*hadError = true
		}
	}
}

// processLease handles reconciliation logic for a single lease.
func (r *Reconciler) processLease(
	ctx context.Context,
	leaseUUID string,
	lease billingtypes.Lease,
	provision backend.ProvisionInfo,
	isProvisioned bool,
	authority reconcileActionAuthority,
	provisioned, acknowledged, anomalies, leaseErrors, deferred *atomic.Int32,
) {
	// Check context before doing any work to respect cancellation
	if ctx.Err() != nil {
		return
	}

	// Track whether this lease hit an unresolved error. Counted once per
	// lease so the aggregate tells operators "how many leases had problems".
	hadError := false
	defer func() {
		if hadError {
			leaseErrors.Add(1)
		}
	}()
	handleStartError := func(err error) {
		if errors.Is(err, errPlacementSnapshotStale) || errors.Is(err, errTrackerSnapshotStale) {
			deferred.Add(1)
			metrics.ReconcilerDeferredLeasesTotal.Inc()
			slog.Debug("reconcile: deferring lease whose placement changed before the provision write-ahead fence",
				"lease_uuid", leaseUUID,
				"inventory_revision", authority.placementRevision,
			)
			return
		}
		r.handleProvisionError(ctx, err, leaseUUID, lease, &hadError)
	}

	payload := payloadEvidenceUnknown
	if lease.State == billingtypes.LEASE_STATE_PENDING && !isProvisioned && len(lease.MetaHash) > 0 {
		// A missing optional payload store historically means "not uploaded";
		// an actual store read error is uncertainty and must remain a deferral.
		payload = payloadEvidenceAbsent
		if r.payloads != nil {
			hasPayload, err := r.payloads.HasPayload(leaseUUID)
			switch {
			case err != nil:
				payload = payloadEvidenceUnknown
				slog.Error("reconcile: failed to check payload store",
					"lease_uuid", leaseUUID,
					"error", err,
				)
				hadError = true
			case hasPayload:
				payload = payloadEvidencePresent
			}
		}
	}

	inFlight := r.isInFlight(leaseUUID)
	plan := planLease(leaseFacts{
		authority:       lifecycleAuthorityDurable,
		chain:           lease.State,
		hasProvision:    isProvisioned,
		provisionStatus: provision.Status,
		failCount:       provision.FailCount,
		maxFailures:     r.maxReprovisionAttempts,
		hasMetaHash:     len(lease.MetaHash) > 0,
		payload:         payload,
		inFlight:        inFlight,
	})

	if plan.anomaly {
		anomalies.Add(1)
	}

	switch plan.action {
	case reconcileActionDefer:
		if inFlight && lease.State == billingtypes.LEASE_STATE_PENDING && isProvisioned &&
			provision.Status == backend.ProvisionStatusReady {
			metrics.ReconcilerInflightSkipsTotal.Inc()
			slog.Debug("reconcile: skipping in-flight ready lease, main flow owns ack",
				"lease_uuid", leaseUUID,
			)
		}

	case reconcileActionWait:
		if plan.reason == "awaiting payload" {
			slog.Debug("reconcile: lease awaiting payload upload",
				"lease_uuid", leaseUUID,
				"tenant", lease.Tenant,
				"meta_hash_hex", fmt.Sprintf("%x", lease.MetaHash),
			)
		} else {
			slog.Debug("reconcile: lease requires no lifecycle action",
				"lease_uuid", leaseUUID,
				"reason", plan.reason,
			)
		}

	case reconcileActionStart:
		if plan.anomaly {
			slog.Warn("reconcile: anomaly requires provisioning",
				"lease_uuid", leaseUUID,
				"tenant", lease.Tenant,
				"backend", provision.BackendName,
				"fail_count", provision.FailCount,
				"max_attempts", r.maxReprovisionAttempts,
				"reason", plan.reason,
			)
		}
		var err error
		if plan.withPayload {
			err = r.startProvisioningWithPayload(ctx, lease, authority)
		} else {
			err = r.startProvisioning(ctx, lease, authority)
		}
		if err != nil {
			handleStartError(err)
		} else if lease.State == billingtypes.LEASE_STATE_PENDING {
			provisioned.Add(1)
		}

	case reconcileActionAcknowledge:
		if err := r.acknowledgeLease(ctx, leaseUUID); err != nil {
			slog.Error("reconcile: failed to acknowledge lease",
				"lease_uuid", leaseUUID,
				"error", err,
			)
			hadError = true
		} else {
			acknowledged.Add(1)
		}

	case reconcileActionReject:
		slog.Warn("reconcile: lease provisioning failed, rejecting",
			"lease_uuid", leaseUUID,
			"tenant", lease.Tenant,
		)
		if err := r.rejectLease(ctx, leaseUUID, plan.reason); err != nil {
			slog.Error("reconcile: failed to reject lease",
				"lease_uuid", leaseUUID,
				"error", err,
			)
			hadError = true
		}

	case reconcileActionCloseAndDeprovision:
		slog.Error("reconcile: provision failed too many times, closing lease",
			"lease_uuid", leaseUUID,
			"tenant", lease.Tenant,
			"backend", provision.BackendName,
			"fail_count", provision.FailCount,
			"max_attempts", r.maxReprovisionAttempts,
		)
		if err := r.closeLease(ctx, leaseUUID, fmt.Sprintf("provision failed %d times", provision.FailCount)); err != nil {
			slog.Error("reconcile: failed to close exhausted lease",
				"lease_uuid", leaseUUID,
				"error", err,
			)
			hadError = true
			return
		}
		if b := r.backendRouter.GetBackendByName(provision.BackendName); b != nil {
			if err := b.Deprovision(ctx, leaseUUID); err != nil {
				slog.Warn("reconcile: failed to deprovision after closing exhausted lease",
					"lease_uuid", leaseUUID,
					"error", err,
				)
			}
		}

	case reconcileActionReconcileCustomDomain:
		b := r.backendRouter.GetBackendByName(provision.BackendName)
		if b == nil {
			// Backend is no longer configured. Same condition the orphan
			// path treats as MANUAL CLEANUP REQUIRED — surface it loudly
			// here too so a misconfigured/removed backend doesn't silently
			// disable custom-domain reconciliation for active leases.
			slog.Error("reconcile: custom_domain reconcile skipped - backend no longer configured",
				"lease_uuid", leaseUUID,
				"tenant", lease.Tenant,
				"backend", provision.BackendName,
			)
			hadError = true
			break
		}
		latestItems := ExtractLeaseItems(&lease)
		if err := b.ReconcileCustomDomain(ctx, leaseUUID, latestItems); err != nil {
			slog.Warn("reconcile: custom_domain reconcile failed; will retry next tick",
				"lease_uuid", leaseUUID,
				"backend", provision.BackendName,
				"error", err,
			)
			hadError = true
		}
	}
}

// processOrphan handles deprovisioning of an orphan provision.
func (r *Reconciler) processOrphan(
	ctx context.Context,
	leaseUUID string,
	provision backend.ProvisionInfo,
	operationSnapshot operation.TrackerSnapshot,
	trackerSnapshotRevision uint64,
	orphans, leaseErrors *atomic.Int32,
) {
	// Check context before doing any work to respect cancellation
	if ctx.Err() != nil {
		return
	}

	// Skip provisions that belong to a different provider
	if provision.ProviderUUID != "" && provision.ProviderUUID != r.providerUUID {
		slog.Debug("reconcile: skipping provision owned by different provider",
			"lease_uuid", leaseUUID,
			"provision_provider", provision.ProviderUUID,
			"our_provider", r.providerUUID,
		)
		return
	}

	// Acquire the same lease capability used by provisioning and close handlers.
	// The inventory-bound claim combines the old in-flight check with a causal
	// fence: an operation that starts and finishes between inventory and this
	// point is still visible as a stale snapshot, while an operation that starts
	// after this point is blocked until teardown returns. This closes both sides
	// of the check-then-Deprovision race (ENG-594).
	release, acquired := r.claimLeaseAction(
		leaseUUID, operationSnapshot, trackerSnapshotRevision,
	)
	if !acquired {
		metrics.ReconcilerInflightSkipsTotal.Inc()
		slog.Debug("reconcile: skipping orphan whose lifecycle crossed the inventory boundary",
			"lease_uuid", leaseUUID,
		)
		return
	}
	defer release()

	// Confirm against the chain that this lease is really finished (ENG-654).
	//
	// Membership of chainLeases is not enough to justify a deprovision. It is
	// derived from two non-atomic, state-filtered list queries, so "absent" means
	// either terminal or never-known, and the in-flight guard above only covers
	// the window while the main flow still owns the lease — not the one after it
	// untracks. This re-read is the positive evidence, and it runs BEFORE the
	// backend lookup so a live lease never trips the MANUAL CLEANUP error below.
	if !r.confirmTerminal(ctx, metrics.CleanupPassOrphan, leaseUUID) {
		return
	}
	// Look up the backend that originally provisioned this resource.
	// We must use the same backend for deprovisioning - falling back to a
	// different backend would fail since it doesn't have the resource.
	b := r.backendRouter.GetBackendByName(provision.BackendName)
	if b == nil {
		// Backend is no longer configured. This orphan requires manual cleanup.
		// Do NOT fall back to default backend - it can't deprovision resources
		// from a different backend system.
		slog.Error("reconcile: orphan provision on unconfigured backend - MANUAL CLEANUP REQUIRED",
			"lease_uuid", leaseUUID,
			"backend", provision.BackendName,
		)
		leaseErrors.Add(1)
		return
	}

	orphans.Add(1)
	slog.Warn("reconcile: orphan provision found, deprovisioning",
		"lease_uuid", leaseUUID,
		"backend", provision.BackendName,
	)

	if err := b.Deprovision(ctx, leaseUUID); err != nil {
		slog.Error("reconcile: failed to deprovision orphan",
			"lease_uuid", leaseUUID,
			"backend", b.Name(),
			"error", err,
		)
		leaseErrors.Add(1)
		return
	}

	// Placement is NOT deleted here (ENG-333): if the backend retained the
	// volumes (RetainOnClose pool), placement must survive so that a restore
	// request can resolve the correct backend. The gated reconciler pruner
	// (cleanupOrphanedPlacements) is the sole owner of placement deletion —
	// it keeps a still-retained lease and prunes a genuinely-gone one once
	// every gate is satisfied.
}

// cleanupOrphanedPayloads removes stored payloads for leases that are no longer pending.
// This handles the case where fred was down when a lease was canceled, so the
// handleLeaseClosed event was missed and the payload wasn't cleaned up.
//
// Returns the number of orphaned payloads cleaned up.
func (r *Reconciler) cleanupOrphanedPayloads(ctx context.Context, chainLeases map[string]billingtypes.Lease) int {
	// Skip if no payload store is available
	payloadStore := r.payloadStore()
	if payloadStore == nil {
		return 0
	}

	// Get all lease UUIDs that have stored payloads
	storedPayloadUUIDs := payloadStore.List()
	if len(storedPayloadUUIDs) == 0 {
		return 0
	}

	cleaned := 0
	for _, leaseUUID := range storedPayloadUUIDs {
		// Check context for cancellation
		if ctx.Err() != nil {
			break
		}

		// Check if the lease exists and is still pending
		lease, exists := chainLeases[leaseUUID]
		if !exists {
			// Absent from the snapshot — which is two non-atomic, state-filtered
			// list queries, so it means terminal OR never-known OR created
			// moments ago. Re-read the lease before deleting (ENG-654): a
			// payload deleted out from under a live lease makes the NEXT sweep
			// see errPayloadNotAvailable, classify it permanent, and close a
			// healthy ACTIVE lease on chain.
			if !r.confirmTerminal(ctx, metrics.CleanupPassPayload, leaseUUID) {
				continue
			}
			payloadStore.Delete(leaseUUID)
			cleaned++
			slog.Info("reconcile: cleaned up orphaned payload (lease terminal on chain, absent from sweep snapshot)",
				"lease_uuid", leaseUUID,
			)
			continue
		}

		// Unreachable in production — chainLeases is built from the PENDING and
		// ACTIVE queries, so anything in it is one of those two. Kept as the
		// belt to the braces above: it acts on a state fred positively read, and
		// costs nothing.
		if lease.State != billingtypes.LEASE_STATE_PENDING && lease.State != billingtypes.LEASE_STATE_ACTIVE {
			// Lease is closed/rejected — payload is no longer needed.
			// ACTIVE leases retain their payload for re-provisioning if the
			// container crashes after the success callback.
			payloadStore.Delete(leaseUUID)
			cleaned++
			slog.Info("reconcile: cleaned up orphaned payload (lease terminal)",
				"lease_uuid", leaseUUID,
				"lease_state", lease.State.String(),
			)
		}
	}

	return cleaned
}

// cleanupOrphanedPlacements is the sole background/age-based pruner of the
// placement index (ENG-333). A successfully rejected PENDING callback can also
// conditionally delete the exact operation-owned record.
// It deletes a placement only when ALL of these hold, so it never races a
// concurrent StartProvisioning Set nor wipes valid placement on a backend
// outage:
//   - every durable candidate owner answered both /provisions and /retentions
//     this sweep. For a confirmed/attempting record those candidates are Backend
//     and Attempt; for a conflict they are every ConflictBackend. Another
//     backend's silence says nothing about the candidate (ENG-654). A removed
//     candidate, a legacy conflict with unknown owners, or a structurally
//     unusable record therefore cannot be auto-pruned: the record is the only
//     pointer to where the data may live (ENG-635);
//   - the lease is absent from backendLeases (provisions ∪ retentions);
//   - the lease is not in-flight (a just-Set placement the backends haven't
//     reported yet — the exact race the old additive-only code avoided);
//   - the lease is chain-terminal: absent from chainLeases, or present but
//     neither PENDING nor ACTIVE (closed/rejected/expired).
//
// Returns the number of placements pruned.
func (r *Reconciler) cleanupOrphanedPlacements(
	ctx context.Context,
	chainLeases map[string]billingtypes.Lease,
	backendLeases map[string]struct{},
	provisionsAnswered answeredSet,
	retentionsAnswered answeredSet,
	now time.Time,
	maxRevision uint64,
	inFlightAtSnapshot map[string]struct{},
	operationSnapshot operation.TrackerSnapshot,
	trackerSnapshotRevision uint64,
) int {
	if r.placementView == nil {
		return 0
	}

	cleaned := 0
	for leaseUUID, record := range r.placementView.List() {
		if ctx.Err() != nil {
			break
		}
		// An ambiguous outbound effect remains live safety evidence even after
		// the chain is terminal and every backend currently reports absence. The
		// remote call can still commit later; pruning its record would permit a
		// future reuse/retry to target another backend with no trace of the first.
		// Only exact refusal, matching positive inventory, or operator repair may
		// remove an Attempt.
		if record.Attempt != "" {
			metrics.ReconcilerCleanupSkipsTotal.
				WithLabelValues(metrics.CleanupPassPlacement, metrics.CleanupSkipAttemptPending).Inc()
			slog.Debug("reconcile: keeping placement with unresolved backend attempt",
				"lease_uuid", leaseUUID,
				"backend", record.Backend,
				"attempt", record.Attempt,
				"operation_id", record.AttemptOperationID().String(),
			)
			continue
		}
		// The backend inventories predate any record newer than the revision
		// captured at fetch start, so they cannot justify deleting it. Likewise, an
		// operation already in flight at that boundary may untrack after an
		// ambiguous response before this late cleanup pass; preserve it for a later
		// inventory that definitely started after the call.
		if r.legacyPlacement != nil && record.Revision() > maxRevision {
			slog.Debug("reconcile: keeping placement newer than fleet inventory",
				"lease_uuid", leaseUUID,
				"record_revision", record.Revision(),
				"inventory_revision", maxRevision,
			)
			continue
		}
		if _, wasInFlight := inFlightAtSnapshot[leaseUUID]; wasInFlight {
			slog.Debug("reconcile: keeping placement whose operation was in flight when inventory began",
				"lease_uuid", leaseUUID)
			continue
		}
		// Every durable candidate must have answered both inventories. A conflict
		// with an unknown/incomplete candidate set and a generic unusable record can
		// never satisfy that proof merely because today's configured fleet answered:
		// an owner removed from configuration is exactly the one we must preserve.
		owners := make([]string, 0, 2+len(record.ConflictBackends))
		addOwner := func(owner string) {
			if owner == "" || slices.Contains(owners, owner) {
				return
			}
			owners = append(owners, owner)
		}
		addOwner(record.Backend)
		addOwner(record.Attempt)
		ownersAccountable := true
		switch {
		case record.Conflict:
			if record.ConflictOwnersUnknown || len(record.ConflictBackends) < 2 {
				ownersAccountable = false
			} else {
				for _, owner := range record.ConflictBackends {
					addOwner(owner)
				}
			}
		case record.State() == placement.StateUnusable:
			ownersAccountable = false
		}
		ownersAnswered := ownersAccountable && len(owners) > 0
		for _, owner := range owners {
			if !provisionsAnswered.heard(owner) || !retentionsAnswered.heard(owner) {
				ownersAnswered = false
				break
			}
		}
		if !ownersAnswered {
			metrics.ReconcilerCleanupSkipsTotal.
				WithLabelValues(metrics.CleanupPassPlacement, metrics.CleanupSkipBackendSilent).Inc()
			slog.Debug("reconcile: keeping placement, a possible owner did not report this sweep",
				"lease_uuid", leaseUUID,
				"backend", record.Backend,
				"attempt", record.Attempt,
				"conflict_backends", record.ConflictBackends,
				"conflict_owners_unknown", record.ConflictOwnersUnknown,
				"state", record.State().String(),
			)
			continue
		}
		// Data still lives on a backend (active provision or retained) → keep.
		if _, onBackend := backendLeases[leaseUUID]; onBackend {
			continue
		}
		// A provision Set this placement moments ago; backends/chain may not
		// reflect it yet → keep (the documented additive-only race).
		if r.isInFlight(leaseUUID) {
			continue
		}
		// Keep if the lease is still PENDING/ACTIVE on chain (the reconciler's
		// main loop owns re-provisioning those; pruning would race it).
		if lease, exists := chainLeases[leaseUUID]; exists &&
			(lease.State == billingtypes.LEASE_STATE_PENDING || lease.State == billingtypes.LEASE_STATE_ACTIVE) {
			continue
		}
		// ENG-335: keep a placement that was set within the grace window. A lease
		// that provisioned entirely during a slow reconcile sweep is absent from
		// this sweep's (stale) snapshot of chain + backends, yet is live; pruning
		// it here strands its volume at close. The placement is a derived index —
		// keeping a young one is harmless (processOrphan GCs the real resource and
		// a closed lease is never restored) — so we never prune within 2× the
		// reconcile interval, comfortably longer than one sweep.
		grace := 2 * r.interval
		if !record.SetAt.IsZero() && grace > 0 && now.Sub(record.SetAt) < grace {
			// Log the raw timestamps rather than a derived age: now is the
			// sweep-start time, so a placement Set during this sweep has
			// set_at > sweep_start (a negative "age") — the timestamps make
			// that case self-explanatory instead of printing a confusing
			// negative duration.
			slog.Debug("reconcile: keeping placement within grace window",
				"lease_uuid", leaseUUID, "set_at", record.SetAt, "sweep_start", now, "grace", grace)
			continue
		}
		// Chain-terminal, absent from all backends, and not in-flight is still
		// only a snapshot conclusion. Claim the lease at the inventory boundary
		// before deleting: a lifecycle operation that crossed the boundary makes
		// this candidate stale, and a delayed event cannot start while the claim
		// is held. Re-read the placement under that fence before the final CAS.
		release, acquired := r.claimLeaseAction(
			leaseUUID, operationSnapshot, trackerSnapshotRevision,
		)
		if !acquired {
			continue
		}
		current := r.placementFor(leaseUUID)
		if current.Revision() != record.Revision() || current.State() != record.State() {
			release()
			continue
		}

		var (
			deleted bool
			err     error
		)
		if r.placementAuthority != nil {
			// New production records and unambiguous v0.13 owners migrated at open
			// carry an opaque revision. A remaining invalid revision therefore names
			// ambiguous or corrupt legacy evidence and cannot authorize deletion.
			if !record.RecordRevision().Valid() {
				release()
				continue
			}
			deleted, err = r.placementAuthority.DeleteRecord(record.RecordRevision())
		} else {
			// Compatibility-only path for legacy mock stores used by the
			// unexported test constructor.
			deleted, err = r.legacyPlacement.DeleteIfRevision(leaseUUID, record.Revision())
		}
		release()
		if err != nil {
			slog.Error("reconcile: failed to prune orphaned placement",
				"lease_uuid", leaseUUID, "error", err)
			continue
		}
		if !deleted {
			slog.Debug("reconcile: placement changed while pruning; keeping newer record",
				"lease_uuid", leaseUUID)
			continue
		}
		cleaned++
		slog.Info("reconcile: pruned orphaned placement", "lease_uuid", leaseUUID)
	}
	return cleaned
}

// Start begins periodic reconciliation.
func (r *Reconciler) Start(ctx context.Context) error {
	// Add jitter (0-25% of interval) to prevent thundering herd when
	// multiple fred instances start simultaneously.
	jitter := time.Duration(rand.Int64N(int64(r.interval / 4))) //nolint:gosec // G404: non-crypto jitter for thundering-herd avoidance; only offsets a startup timer, never security-sensitive
	slog.Info("starting periodic reconciliation",
		"interval", r.interval,
		"initial_jitter", jitter,
	)

	// Wait for initial jitter before starting ticker
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(jitter):
	}

	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			slog.Info("periodic reconciliation stopped")
			return ctx.Err()

		case <-ticker.C:
			if err := r.ReconcileAll(ctx); err != nil {
				slog.Error("periodic reconciliation failed", "error", err)
				// Continue - don't stop periodic reconciliation on error
			}
		}
	}
}

// RunOnce performs a single reconciliation. Use this at startup.
func (r *Reconciler) RunOnce(ctx context.Context) error {
	return r.ReconcileAll(ctx)
}
