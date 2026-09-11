package provisioner

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"math/rand/v2"
	"runtime/debug"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backendidentity"
	"github.com/manifest-network/fred/internal/metrics"
	"github.com/manifest-network/fred/internal/provisioner/payload"
	"github.com/manifest-network/fred/internal/provisioner/placement"
	"github.com/manifest-network/fred/internal/util"
)

// Default concurrency limits for reconciliation.
const (
	// DefaultReconcileWorkers is the default number of concurrent workers for
	// processing leases and orphans during reconciliation.
	DefaultReconcileWorkers = 10
)

// chainConfirmTimeout bounds ONE per-candidate lease lookup in
// getLeaseBounded, shared by lifecycle dispatch, destructive cleanup
// confirmation, and conservative placement-marker retirement.
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

// chainInventoryTimeout bounds ONE complete paginated PENDING or ACTIVE lease
// inventory. Each state receives an independently derived context: exhausting
// the pending budget must not consume the active inventory's opportunity to
// answer. A failed list invalidates the whole sweep, so the reconciler keeps
// ReconcilerSweepComplete at zero and retries from a fresh snapshot next tick.
//
// This is deliberately longer than chainConfirmTimeout because it covers every
// page in one state inventory rather than one point query. It is still a fixed
// liveness backstop, not an operator tuning knob. Both reads run concurrently,
// so one such wall-clock budget bounds this phase before startup or the periodic
// loop regains control.
const chainInventoryTimeout = 30 * time.Second

// placementCleanupTimeout is one wall-clock budget for the complete placement
// cleanup pass. Individual exact reads remain bounded too, but a fleet of
// stalled candidates cannot multiply that bound by its size.
const placementCleanupTimeout = 10 * time.Second

// errLeaseAlreadyInFlight indicates the lease is already being provisioned.
// This is not a real error - the caller should not treat it as a failure.
var errLeaseAlreadyInFlight = errors.New("lease already in-flight")

// errPlacementSnapshotStale means a placement mutation crossed the inventory
// boundary before the reconciler's write-ahead attempt could commit. The lease
// is retried from a newer snapshot; no backend call was made.
var errPlacementSnapshotStale = errors.New("placement changed after inventory snapshot")

var errTrackerSnapshotStale = errors.New("in-flight operation changed after inventory snapshot")

// reconcileActionAuthority carries the exact process-local and durable
// capabilities that authorize one action planned from a fleet snapshot. Every
// field is an opaque, safe-zero capability issued by the operation or placement
// authority; raw numeric revisions cannot authorize a backend side effect.
type reconcileActionAuthority struct {
	action placement.ObservedReconciliationAction
}

// errPayloadNotAvailable indicates that the payload required for provisioning
// is not currently available from the durable payload store. Absence is not
// tenant-invalidity evidence: reconciliation preserves the lease and retries
// after an upload or database repair.
var errPayloadNotAvailable = errors.New("payload not available")

// ReconcilerPayloads is the payload capability consumed by reconciliation.
// Lifecycle coordination lives in operation.Registry; payload persistence is a
// separate concern and deliberately does not expose registry mutation.
type ReconcilerPayloads interface {
	HasPayload(leaseUUID string) (bool, error)
	PayloadStore() *payload.Store
}

// ReconcilerChainClient is retained as the public name used by backend
// integration fixtures. Reconciliation construction accepts this capability
// only once, when minting placement.ReconciliationCoordinator.
type ReconcilerChainClient = placement.ReconciliationChain

// Reconciler performs level-triggered reconciliation between chain state and backend state.
// It ensures consistency by comparing current state rather than replaying events.
type Reconciler struct {
	payloads        ReconcilerPayloads
	coordinator     *placement.ReconciliationCoordinator
	attemptRecovery *attemptRecoveryCoordinator
	placementPruner *placementPruner

	interval               time.Duration
	maxWorkers             int           // Maximum concurrent workers for lease processing
	maxReprovisionAttempts int           // Max re-provision attempts before rejecting
	chainInventoryBudget   time.Duration // Whole-list timeout; fixed in production, shortened by tests.
	placementCleanupBudget time.Duration // Whole-pass timeout; fixed in production, shortened by tests.
	reconciling            atomic.Bool   // Non-blocking flag to prevent concurrent reconciliation
	placementSweepSeen     atomic.Bool   // True while a durable baseline matches the configured backend topology.
}

// DefaultMaxReprovisionAttempts is the default number of re-provision attempts
// before rejecting a lease whose containers keep failing.
const DefaultMaxReprovisionAttempts = 3

// ReconcilerConfig configures the reconciler.
type ReconcilerConfig struct {
	Interval               time.Duration // How often to run periodic reconciliation
	MaxWorkers             int           // Maximum concurrent workers (default: 10)
	MaxReprovisionAttempts int           // Max re-provision attempts before rejecting (default: 3)
	Coordinator            *placement.ReconciliationCoordinator
}

// NewReconciler creates the production reconciler. Placement authority and the
// shared operation registry are mandatory: every supported deployment uses a
// multi-backend router, so constructing a reconciler that can issue an
// unrecorded or unfenced backend mutation is invalid.
func NewReconciler(
	cfg ReconcilerConfig,
	payloads ReconcilerPayloads,
) (*Reconciler, error) {
	if util.IsNilInterface(payloads) {
		return nil, errors.New("reconciler payloads are required")
	}
	coordinator := cfg.Coordinator
	if coordinator == nil || !coordinator.Valid() {
		return nil, errors.New("router-bound reconciliation coordinator is required")
	}
	reconciliation := coordinator
	attemptRecovery := newAttemptRecoveryCoordinator(reconciliation)
	_, recoveryAuthority, err := reconciliation.BindRecovery()
	if err != nil {
		return nil, fmt.Errorf("bind reconciler recovery authority: %w", err)
	}
	attemptRecovery.recovery = recoveryAuthority
	// Apply defaults using cmp.Or (returns first non-zero value).
	interval := cmp.Or(cfg.Interval, 5*time.Minute)
	maxWorkers := cmp.Or(max(cfg.MaxWorkers, 0), DefaultReconcileWorkers)
	maxReprovision := cmp.Or(max(cfg.MaxReprovisionAttempts, 0), DefaultMaxReprovisionAttempts)
	reconciler := &Reconciler{
		payloads:               payloads,
		coordinator:            reconciliation,
		attemptRecovery:        attemptRecovery,
		interval:               interval,
		maxWorkers:             maxWorkers,
		maxReprovisionAttempts: maxReprovision,
		chainInventoryBudget:   chainInventoryTimeout,
		placementCleanupBudget: placementCleanupTimeout,
	}
	reconciler.placementPruner = newPlacementPruner(
		reconciliation, reconciler.attemptRecovery,
		interval, maxWorkers,
	)
	reconciler.placementSweepSeen.Store(reconciliation.AdmissionReady())
	return reconciler, nil
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

	slog.Info("starting reconciliation", "provider_uuid", r.coordinator.ProviderUUID())
	// Sweep completeness describes current observability only. Beginning the
	// typed inventory session invalidates older projection proofs, while the
	// separately persisted topology baseline remains valid through a transient
	// backend outage.
	metrics.ReconcilerSweepComplete.Set(0)

	// Chain reads carry no backend ownership evidence. Finish them before
	// registering the durable inventory marker so a transient chain failure
	// cannot manufacture an interrupted backend sweep and withdraw admission
	// until every backend is reachable again.
	pendingLeases, activeLeases, err := r.collectChainLeaseInventory(ctx)
	if err != nil {
		return err
	}

	// Capture the Store fence, operation boundary, and collector epoch as one
	// opaque capability immediately before the first backend read. No later
	// phase can combine backend facts from different reconciliation sweeps.
	sweep, err := r.coordinator.BeginSweep()
	if err != nil {
		return fmt.Errorf("begin reconciliation sweep: %w", err)
	}
	defer sweep.End()

	// Collection is read-only. Its facts become authority only after the atomic
	// placement projection below commits successfully.
	inventory, err := r.collectInventory(ctx, sweep, pendingLeases, activeLeases)
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

	// Retained leases also pin a backend (restore affinity, ENG-333).
	allRetentions := inventory.retentions
	retentionsAnswered := inventory.retentionsAnswered
	slog.Info("fetched backend retentions",
		"total", len(allRetentions),
		"complete", retentionsAnswered.complete(),
		"unanswered", retentionsAnswered.unanswered(),
	)
	projection, err := r.projectPlacementInventory(ctx, reconcileProjectionInput{
		inventory: inventory,
		sweep:     sweep,
	})
	if err != nil {
		return err
	}
	placementSyncOK := projection.syncOK
	placementRecords := projection.records
	ambiguousOwners := projection.ambiguousOwners

	// A fleet snapshot is authoritative only when both independent backend
	// inventories are complete and every positive observation/conflict has
	// crossed the durable placement boundary. Provision inventory alone is not
	// enough: retained data also pins ownership, and an unpersisted projection
	// cannot safely authorize lifecycle work or advance the success heartbeat.
	cycleComplete := projection.projected != nil &&
		projection.projected.Complete() && placementSyncOK
	if cycleComplete {
		metrics.ReconcilerSweepComplete.Set(1)
	} else {
		metrics.ReconcilerSweepComplete.Set(0)
	}
	admissionBaseline := placement.AdmissionBaseline{}
	if projection.projected != nil {
		admissionBaseline = projection.projected.AdmissionBaseline()
	}
	r.placementSweepSeen.Store(admissionBaseline.Valid())
	// A degraded sweep may place genuinely new work only on a backend that
	// answered both inventories. This is an immutable per-sweep hard boundary;
	// routing fallback is not allowed to escape it.
	eligibleBackends := make(map[string]struct{})
	if projection.projected != nil {
		for _, backendName := range projection.projected.EligibleBackends() {
			eligibleBackends[backendName] = struct{}{}
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
		// errPayloadNotAvailable. That condition is deliberately retriable, but
		// deleting the only local request authority would still strand a healthy
		// ACTIVE lease indefinitely. Skip the goroutine body; leave the maps alone.
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
			deferForSnapshotBoundary := func(reason string) {
				deferred.Add(1)
				metrics.ReconcilerDeferredLeasesTotal.Inc()
				slog.Debug("reconcile: deferring lease whose operation crossed the fleet snapshot boundary",
					"lease_uuid", leaseUUID,
					"reason", reason,
					"placement_revision", placementRecord.Revision(),
				)
			}
			if sweep.WasInFlight(leaseUUID) {
				if lease.State == billingtypes.LEASE_STATE_PENDING && isProvisioned &&
					provision.Status == backend.ProvisionStatusReady {
					metrics.ReconcilerInflightSkipsTotal.Inc()
				}
				deferForSnapshotBoundary("in_flight_at_inventory_start")
				return nil
			}
			if projection.projected == nil {
				deferForSnapshotBoundary("inventory_projection_unavailable")
				return nil
			}
			if metadata := placementRecord.AttemptMetadata(); metadata.Valid() {
				result := r.attemptRecovery.Redeliver(gctx, leaseUUID, projection.projected)
				switch result.outcome {
				case attemptRedeliveryAccepted:
					if lease.State == billingtypes.LEASE_STATE_PENDING {
						provisioned.Add(1)
					}
				case attemptRedeliveryRefused:
					// The exact contract proved no side effect was accepted. The
					// durable gate is now clear; ordinary reconciliation owns the
					// next lifecycle decision from a fresh fleet snapshot.
				default:
					deferred.Add(1)
					leaseErrors.Add(1)
					metrics.ReconcilerDeferredLeasesTotal.Inc()
					slog.Warn("reconcile: exact durable operation redelivery deferred",
						"lease_uuid", leaseUUID,
						"backend", placementRecord.Attempt,
						"operation_fingerprint", metadata.OperationID(),
						"operation_kind", metadata.Kind(),
						"error", result.err,
					)
				}
				return nil
			}
			if r.coordinator.AbsenceUntrusted(leaseUUID) {
				deferForSnapshotBoundary("placement_observation_excluded")
				return nil
			}
			allowRecordless := cycleComplete ||
				(placementSyncOK && admissionBaseline.Valid() &&
					lease.State == billingtypes.LEASE_STATE_PENDING && len(eligibleBackends) > 0)
			absenceTrusted := allowRecordless

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
			observed, disposition, err := projection.projected.ObserveLiveAction(gctx, leaseUUID)
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
			if disposition != placement.ReconciliationObservationReady || !observed.Valid() {
				deferred.Add(1)
				metrics.ReconcilerDeferredLeasesTotal.Inc()
				slog.Debug("reconcile: lease changed after inventory; skipping lifecycle action",
					"lease_uuid", leaseUUID,
					"observation_disposition", disposition,
				)
				return nil
			}
			defer r.coordinator.ReleaseAction(observed)
			lease = observed.Lease()
			authority := reconcileActionAuthority{action: observed}

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

	for leaseUUID := range allProvisions {
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
			r.processOrphan(ogctx, leaseUUID, projection.projected, &orphans, &leaseErrors)
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
		ctx, chainLeases, projection.projected, startTime,
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
		"placement_sync_ok", placementSyncOK,
		"placement_absence_trusted", r.placementSweepSeen.Load(),
		"orphaned_payloads_cleaned", orphanedPayloads,
		"orphaned_placements_pruned", prunedPlacements,
	)

	return nil
}

// provisionStartDisposition is the reconciler-local projection of one complete
// provision transaction. Its zero value is invalid: callers cannot accidentally
// treat an uninitialized result as a successful dispatch.
type provisionStartDisposition uint8

const (
	provisionStartInvalid provisionStartDisposition = iota
	provisionStartAccepted
	provisionStartUncertain
	provisionStartValidationRefused
)

// provisionStartResult carries the causal distinction that an error tree
// cannot express. In particular, only a sealed transport refusal can mint
// provisionStartValidationRefused; a legacy Backend returning an error that
// happens to wrap backend.ErrValidation remains uncertain.
type provisionStartResult struct {
	disposition provisionStartDisposition
	err         error
}

func acceptedProvisionStart() provisionStartResult {
	return provisionStartResult{disposition: provisionStartAccepted}
}

func uncertainProvisionStart(err error) provisionStartResult {
	if err == nil {
		err = errors.New("provision start failed without a cause")
	}
	return provisionStartResult{disposition: provisionStartUncertain, err: err}
}

// failedProvisionStart projects a placement-minted dispatch result. Because
// DispatchResult's causal fields are private to placement, no caller in this
// broader package can manufacture terminal validation authority from an error.
func failedProvisionStart(settlement placement.DispatchResult) provisionStartResult {
	err := settlement.CallErr()
	if err == nil {
		err = settlement.Err()
	}
	if err == nil {
		err = errors.New("provision dispatch failed without a cause")
	}
	if settlement.ProvisionRefusal() != backend.ProvisionRefusalValidation {
		return uncertainProvisionStart(err)
	}
	return provisionStartResult{disposition: provisionStartValidationRefused, err: err}
}

func (result provisionStartResult) valid() bool {
	switch result.disposition {
	case provisionStartAccepted:
		return result.err == nil
	case provisionStartUncertain, provisionStartValidationRefused:
		return result.err != nil
	default:
		return false
	}
}

func (result provisionStartResult) Err() error {
	if !result.valid() {
		return errors.New("invalid provision start result")
	}
	return result.err
}

func (result provisionStartResult) validationRefused() bool {
	return result.valid() && result.disposition == provisionStartValidationRefused
}

// startProvisioning initiates provisioning for a lease without a payload.
// Its typed result preserves whether the transport itself proved a validation
// refusal; arbitrary errors are never upgraded into that authority.
func (r *Reconciler) startProvisioning(
	ctx context.Context,
	lease billingtypes.Lease,
	authority reconcileActionAuthority,
) provisionStartResult {
	return r.doStartProvisioning(ctx, lease, false, authority)
}

// startProvisioningWithPayload initiates provisioning for a lease that requires a payload.
// Returns errLeaseAlreadyInFlight if the lease is already being provisioned.
func (r *Reconciler) startProvisioningWithPayload(
	ctx context.Context,
	lease billingtypes.Lease,
	authority reconcileActionAuthority,
) provisionStartResult {
	return r.doStartProvisioning(ctx, lease, true, authority)
}

// doStartProvisioning is the common implementation for provisioning with or without payload.
func (r *Reconciler) doStartProvisioning(
	ctx context.Context,
	lease billingtypes.Lease,
	withPayload bool,
	authority reconcileActionAuthority,
) provisionStartResult {
	if len(lease.MetaHash) != 0 && !withPayload {
		return uncertainProvisionStart(fmt.Errorf(
			"%w: payload-bearing lease %s reached payloadless reconciliation",
			errPayloadNotAvailable, lease.Uuid,
		))
	}
	if !authority.action.Valid() || authority.action.Lease().Uuid != lease.Uuid {
		return uncertainProvisionStart(errTrackerSnapshotStale)
	}
	var requestPayload []byte
	attemptPayloadFingerprint := placement.PayloadFingerprint{}

	// Get the payload from the store WITHOUT removing it yet.
	// We only delete after Provision() succeeds to allow retries.
	// Only include PayloadHash when we have the actual payload - this ensures
	// backends never receive a hash without the corresponding data.
	if withPayload {
		payloadStore := r.payloadStore()
		if payloadStore == nil {
			if len(lease.MetaHash) > 0 {
				return uncertainProvisionStart(fmt.Errorf(
					"%w: lease %s", errPayloadNotAvailable, lease.Uuid,
				))
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
			requestPayload, recordedHash, getErr = payloadStore.GetWithHash(lease.Uuid)
			if getErr != nil {
				// Database error, or a recorded hash that is not a SHA-256 — do NOT
				// treat either as "payload missing". Abort this provision attempt so
				// a transient disk issue doesn't cause us to close an active lease.
				return uncertainProvisionStart(fmt.Errorf(
					"failed to read payload for lease %s: %w", lease.Uuid, getErr,
				))
			}
			if requestPayload == nil && len(lease.MetaHash) > 0 {
				// Payload is required (lease has MetaHash) but not in the store.
				// This can happen if the payload DB was lost or fred restarted
				// without its data. We cannot re-provision without the manifest.
				return uncertainProvisionStart(fmt.Errorf(
					"%w: lease %s", errPayloadNotAvailable, lease.Uuid,
				))
			}
			if requestPayload != nil {
				// Re-verify the payload before provisioning to catch corruption.
				//
				// Verify against the hash recorded when the payload was written, not
				// against the lease's on-chain MetaHash. MetaHash is set once at
				// lease creation and is immutable, so it names the manifest the
				// lease was CREATED with — while a tenant /update legitimately
				// replaces the stored manifest without changing it (ENG-619).
				// Checking an updated payload against MetaHash would read a
				// successful update as corruption, delete the payload, and then
				// strand the ACTIVE lease behind errPayloadNotAvailable.
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
				if len(expectedHash) == 0 {
					computed := payload.ComputeHash(requestPayload)
					expectedHash = computed
					verifiedAgainst = "computed_hash"
				}

				if err := payload.VerifyHash(requestPayload, expectedHash); err != nil {
					// Payload is corrupted - delete it and fail
					payloadStore.Delete(lease.Uuid)
					slog.Error("reconcile: payload hash mismatch - possible corruption",
						"lease_uuid", lease.Uuid,
						"verified_against", verifiedAgainst,
						"error", err,
					)
					return uncertainProvisionStart(err)
				}
				// The hash sent to the backend describes the payload actually being
				// sent, which after an update is no longer MetaHash.
				var fingerprintErr error
				attemptPayloadFingerprint, fingerprintErr = placement.NewPayloadFingerprint(expectedHash)
				if fingerprintErr != nil {
					return uncertainProvisionStart(fmt.Errorf(
						"bind exact provision payload fingerprint: %w", fingerprintErr,
					))
				}
			}
		}
	}
	result := r.coordinator.Provision(
		ctx, authority.action, requestPayload, attemptPayloadFingerprint,
	)
	if result.Err() != nil {
		switch {
		case errors.Is(result.Err(), placement.ErrReconciliationBoundaryStale):
			return uncertainProvisionStart(errTrackerSnapshotStale)
		case errors.Is(result.Err(), placement.ErrReconciliationOperationBusy),
			errors.Is(result.Err(), placement.ErrAttemptConflict):
			metrics.ReconciliationConflictsTotal.Inc()
			return uncertainProvisionStart(errLeaseAlreadyInFlight)
		default:
			return uncertainProvisionStart(result.Err())
		}
	}
	settlement := result.Dispatch()
	provisionErr := settlement.CallErr()
	if provisionErr == nil && settlement.Err() != nil {
		provisionErr = settlement.Err()
	}
	if errors.Is(provisionErr, backend.ErrInsufficientResources) {
		metrics.BackendInsufficientResourcesTotal.WithLabelValues(
			result.BackendName(), capacityVerdictLabel(settlement.ProvisionRefusal()),
		).Inc()
	}
	if settlement.Superseded() {
		// The exact callback owns every terminal side effect. In particular,
		// a synchronous error arriving after an inline Ready/Failed verdict
		// cannot clear its placement or trigger chain error handling.
		slog.Info("reconcile: inline provision callback superseded synchronous backend result",
			"lease_uuid", lease.Uuid,
			"backend", result.BackendName(),
			"call_accepted", settlement.CallAccepted(),
			"call_definitively_refused", settlement.CallDefinitivelyRefused(),
			"call_not_dispatched", settlement.CallNotDispatched(),
			"call_ambiguous", settlement.CallAmbiguous(),
		)
		return acceptedProvisionStart()
	}
	settleErr := settlement.Err()
	if settlement.Disposition() == placement.DispatchInvalid && settleErr == nil {
		settleErr = errors.New("invalid provision dispatch disposition")
	}
	if settleErr != nil {
		slog.Warn("reconcile: failed to settle provision placement",
			"lease_uuid", lease.Uuid,
			"backend", result.BackendName(),
			"call_accepted", settlement.CallAccepted(),
			"call_definitively_refused", settlement.CallDefinitivelyRefused(),
			"call_not_dispatched", settlement.CallNotDispatched(),
			"call_ambiguous", settlement.CallAmbiguous(),
			"error", settleErr,
		)
	}

	if settlement.CallAccepted() {
		if settleErr != nil {
			// The backend accepted the operation. Keep the registry operation and durable
			// Attempt so neither event retries nor a degraded sweep can substitute
			// another backend; callback/SetBatch will repair confirmation.
			return uncertainProvisionStart(fmt.Errorf(
				"confirm accepted provision placement for lease %s: %w", lease.Uuid, settleErr,
			))
		}
	} else {
		// A definitive failure cleared Attempt when persistence succeeded. An
		// ambiguous result keeps it. Either way the durable record, rather than
		// the ephemeral registry, gates the next call; releasing the operation lets the
		// reconciler consume a later authoritative inventory snapshot.
		if settlement.CallAmbiguous() {
			slog.Warn("reconcile: provision returned an ambiguous outcome; retaining exact durable attempt",
				"lease_uuid", lease.Uuid,
				"backend", result.BackendName(),
				"error", provisionErr,
			)
		}
		return failedProvisionStart(settlement)
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
			"sku", ExtractRoutingSKU(&lease),
			"backend", result.BackendName(),
			"payload_size", len(requestPayload),
		)
	} else {
		slog.Info("reconcile: started provisioning",
			"lease_uuid", lease.Uuid,
			"tenant", lease.Tenant,
			"sku", ExtractRoutingSKU(&lease),
			"backend", result.BackendName(),
		)
	}

	return acceptedProvisionStart()
}

// acknowledgeLease acknowledges the exact lease carried by the projected
// action through the control plane bound to this coordinator.
func (r *Reconciler) acknowledgeLease(
	ctx context.Context,
	action placement.ObservedReconciliationAction,
) error {
	leaseUUID := action.Lease().Uuid
	acknowledged, txHash, err := r.coordinator.AcknowledgeObserved(ctx, action)
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
func (r *Reconciler) rejectLease(
	ctx context.Context,
	action placement.ObservedReconciliationAction,
	reason string,
) error {
	leaseUUID := action.Lease().Uuid
	rejected, txHashes, err := r.coordinator.RejectObserved(
		ctx, action, truncateRejectReason(reason),
	)
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
func (r *Reconciler) closeLease(
	ctx context.Context,
	action placement.ObservedReconciliationAction,
	reason string,
) error {
	leaseUUID := action.Lease().Uuid
	closed, txHashes, err := r.coordinator.CloseObserved(ctx, action, reason)
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

func placementConflicts(records map[string]placement.Placement) map[string][]string {
	conflicts := make(map[string][]string)
	for leaseUUID, record := range records {
		if !record.Conflict {
			continue
		}
		owners := slices.Clone(record.ConflictBackends)
		if record.Backend != "" {
			owners = append(owners, record.Backend)
		}
		if record.Attempt != "" {
			owners = append(owners, record.Attempt)
		}
		slices.Sort(owners)
		owners = slices.Compact(owners)
		conflicts[leaseUUID] = owners
	}
	return conflicts
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
	// provisionsByBackend retains each endpoint response before the union loses
	// duplicate identities. It is the exact row set sealed into inventory
	// evidence, including lifecycle generations and runtime principals.
	provisionsByBackend map[string][]backend.ProvisionInfo
	// collectedByBackend retains the opaque, exact-sweep response consumed by
	// the typed inventory session after cross-endpoint validation.
	collectedByBackend map[string]placement.BackendProvisionInventory
	// reportedByBackend retains the per-backend membership that the union above
	// intentionally flattens. Placement attempts use it to distinguish "the
	// attempted backend answered without this lease" from "some other backend
	// reported the same lease".
	reportedByBackend map[string]map[string]struct{}
	// storageIdentities records the immutable substrate identity observed on the
	// complete provision inventory from each backend.
	storageIdentities map[string]backendidentity.ID
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
func (r *Reconciler) fetchFleetSnapshot(
	ctx context.Context,
	sweep *placement.ReconciliationSweep,
) fleetSnapshot {
	backendNames, namesErr := r.coordinator.BackendNames()
	if namesErr != nil {
		slog.Error("reconciler cannot enumerate bound backends", "error", namesErr)
		return fleetSnapshot{
			provisions:          make(map[string]backend.ProvisionInfo),
			provisionsByBackend: make(map[string][]backend.ProvisionInfo),
			collectedByBackend:  make(map[string]placement.BackendProvisionInventory),
			reportedByBackend:   make(map[string]map[string]struct{}),
			storageIdentities:   make(map[string]backendidentity.ID),
			answered:            make(answeredSet),
			complete:            false,
		}
	}

	g, gctx := errgroup.WithContext(ctx)
	if len(backendNames) > 0 {
		g.SetLimit(len(backendNames)) // Query all backends concurrently
	}

	var mu sync.Mutex
	snap := fleetSnapshot{
		provisions:          make(map[string]backend.ProvisionInfo),
		provisionsByBackend: make(map[string][]backend.ProvisionInfo, len(backendNames)),
		collectedByBackend:  make(map[string]placement.BackendProvisionInventory, len(backendNames)),
		reportedByBackend:   make(map[string]map[string]struct{}, len(backendNames)),
		storageIdentities:   make(map[string]backendidentity.ID, len(backendNames)),
		answered:            make(answeredSet, len(backendNames)),
		complete:            true,
	}

	for _, backendName := range backendNames {
		g.Go(func() error {
			inventory, err := sweep.CollectProvisionInventory(gctx, backendName)
			if err != nil {
				slog.Error("failed to list provisions from backend",
					"backend", backendName,
					"error", err,
				)
				mu.Lock()
				snap.markUnanswered(backendName)
				mu.Unlock()
				outcome := metrics.FetchOutcomeError
				if errors.Is(err, backend.ErrCircuitOpen) {
					outcome = metrics.FetchOutcomeCircuitOpen
				}
				metrics.ReconcilerBackendFetchTotal.WithLabelValues(backendName, outcome).Inc()
				return nil
			}
			refreshErr := inventory.RefreshErr()
			if refreshErr != nil {
				slog.Warn("failed to refresh backend state",
					"backend", backendName, "error", refreshErr,
				)
			}

			provisions := inventory.Provisions()
			mu.Lock()
			// The immutable storage identity is useful even when RefreshState
			// failed. The caller pairs it with the independent retention identity
			// before preserving stale positive affinity; answered remains false so
			// this response can never authorize negative evidence or completeness.
			snap.storageIdentities[backendName] = inventory.StorageID()
			if refreshErr == nil {
				snap.answered[backendName] = true
			} else {
				snap.markUnanswered(backendName)
			}
			reported := make(map[string]struct{}, len(provisions))
			for _, p := range provisions {
				snap.provisions[p.LeaseUUID] = p
				reported[p.LeaseUUID] = struct{}{}
			}
			snap.provisionsByBackend[backendName] = provisions
			snap.collectedByBackend[backendName] = inventory
			snap.reportedByBackend[backendName] = reported
			mu.Unlock()
			fetchOutcome := metrics.FetchOutcomeOK
			if refreshErr != nil {
				fetchOutcome = metrics.FetchOutcomeError
				if errors.Is(refreshErr, backend.ErrCircuitOpen) {
					fetchOutcome = metrics.FetchOutcomeCircuitOpen
				}
			}
			metrics.ReconcilerBackendFetchTotal.WithLabelValues(backendName, fetchOutcome).Inc()

			slog.Debug("fetched backend provisions",
				"backend", backendName,
				"count", len(provisions),
			)
			return nil
		})
	}

	_ = g.Wait() // closures never return non-nil; outcomes are recorded in snap

	return snap
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
	// An unresolved attempt is an execution gate, not an affinity pin. Only its
	// exact callback, an exact paired-generation inventory observation, a trusted
	// synchronous refusal, or explicit operator repair can settle it; absence is
	// never causal proof that the call did not commit.
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

// getLeaseBounded applies the reconciler's per-query liveness budget to an
// exact lease read. ReconcileAll normally receives the process-lifetime
// context, so relying on its deadline would let one stalled RPC retain a lease
// claim and the sweep-wide reconciliation guard indefinitely.
func (r *Reconciler) getLeaseBounded(
	ctx context.Context,
	leaseUUID string,
) (*billingtypes.Lease, error) {
	qctx, cancel := context.WithTimeout(ctx, chainConfirmTimeout)
	defer cancel()

	return r.coordinator.ReadLease(qctx, leaseUUID)
}

// queryLeaseLiveness performs the bounded point read shared by destructive
// cleanup confirmation and conservative placement-marker retirement. Keeping
// the query separate from either caller's instrumentation prevents bookkeeping
// checks from masquerading as withheld destructive cleanup.
func (r *Reconciler) queryLeaseLiveness(
	ctx context.Context,
	leaseUUID string,
) (leaseLiveness, string, *billingtypes.Lease, error) {
	lease, err := r.getLeaseBounded(ctx, leaseUUID)
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
func (r *Reconciler) fetchAllRetentions(
	ctx context.Context,
	sweep *placement.ReconciliationSweep,
) (map[string]string, answeredSet, map[string]map[string]struct{}, map[string]backendidentity.ID, map[string]placement.BackendRetentionInventory) {
	backendNames, namesErr := r.coordinator.BackendNames()
	if namesErr != nil {
		slog.Error("reconciler cannot enumerate retention backends", "error", namesErr)
		return map[string]string{}, answeredSet{}, map[string]map[string]struct{}{}, map[string]backendidentity.ID{}, map[string]placement.BackendRetentionInventory{}
	}

	var mu sync.Mutex
	out := make(map[string]string)
	answered := make(answeredSet, len(backendNames))
	reportedByBackend := make(map[string]map[string]struct{}, len(backendNames))
	storageIdentities := make(map[string]backendidentity.ID, len(backendNames))
	collected := make(map[string]placement.BackendRetentionInventory, len(backendNames))

	g, gctx := errgroup.WithContext(ctx)
	if len(backendNames) > 0 {
		g.SetLimit(len(backendNames))
	}
	for _, backendName := range backendNames {
		g.Go(func() error {
			inventory, err := sweep.CollectRetentionInventory(gctx, backendName)
			if err != nil {
				slog.Warn("failed to list retentions from backend",
					"backend", backendName, "error", err)
				mu.Lock()
				answered[backendName] = false
				mu.Unlock()
				return nil // collect from other backends; don't cancel
			}
			retentions := inventory.Retentions()
			reported := make(map[string]struct{}, len(retentions))
			for _, ret := range retentions {
				reported[ret.LeaseUUID] = struct{}{}
			}
			mu.Lock()
			answered[backendName] = true
			storageIdentities[backendName] = inventory.StorageID()
			collected[backendName] = inventory
			reportedByBackend[backendName] = reported
			for _, ret := range retentions {
				out[ret.LeaseUUID] = backendName
			}
			mu.Unlock()
			slog.Debug("fetched backend retentions", "backend", backendName, "count", len(retentions))
			return nil
		})
	}
	_ = g.Wait() // closures never return non-nil; outcomes are recorded in answered

	return out, answered, reportedByBackend, storageIdentities, collected
}

// handleProvisionResult handles non-successful provisioning attempts during
// reconciliation. Retry/defer policy may inspect diagnostic errors, but the
// only result authorized to terminate a lease is the typed validation refusal
// minted from the sealed backend transport outcome.
//
// It determines the appropriate action based on result and error type:
//   - errLeaseAlreadyInFlight: skip (not a real error)
//   - backend.ErrAlreadyProvisioned: transient (an unvalidated HTTP 409 is ambiguous)
//   - errPayloadNotAvailable: transient; retain the lease and retry after repair/upload
//   - provisionStartValidationRefused: reject (PENDING) or close (ACTIVE)
//   - backend.ErrMalformedErrorBody: transient (backend answered off-contract) — flag for retry, never terminate
//   - ErrPlacementUnresolvable: transient (backend absent from config) — flag for retry, never terminate
//   - backend.ErrCircuitOpen: transient (breaker auto-recovers) — flag for retry, never terminate
//   - other errors: log and flag for retry next cycle
func (r *Reconciler) handleProvisionResult(
	ctx context.Context,
	result provisionStartResult,
	leaseUUID string,
	lease billingtypes.Lease,
	action placement.ObservedReconciliationAction,
	hadError *bool,
) {
	err := result.Err()
	placementRecord := action.Placement()
	if errors.Is(err, errLeaseAlreadyInFlight) {
		slog.Debug("reconcile: lease already in-flight, skipping", "lease_uuid", leaseUUID)
		return
	}
	if errors.Is(err, backend.ErrAlreadyProvisioned) {
		// HTTPClient maps an unvalidated 409 to this sentinel, so it is not durable
		// ownership proof. The write-ahead Attempt prevents substitution; only an
		// exact callback or a later upgraded inventory report carrying the same
		// paired typed generation settles it.
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
		slog.Error("reconcile: refusing to provision, lease is placed on a backend the router does not know",
			"lease_uuid", leaseUUID,
			"tenant", lease.Tenant,
			"placement_backend", placementRecord.Backend,
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
	if errors.Is(err, errPayloadNotAvailable) {
		// The payload database is durable request authority. Its absence is not
		// proof that a tenant request is invalid and must never reject or close a
		// live lease. Keep reconciliation level-triggered so an upload or database
		// restore repairs the condition without reallocating an operation.
		slog.Error("reconcile: exact payload unavailable, will retry next cycle",
			"lease_uuid", leaseUUID,
			"tenant", lease.Tenant,
			"error", err,
		)
		*hadError = true
		return
	}

	if !result.validationRefused() {
		// Transient error — log and retry next cycle
		slog.Error("reconcile: provisioning failed",
			"lease_uuid", leaseUUID,
			"tenant", lease.Tenant,
			"error", err,
		)
		*hadError = true
		return
	}
	reason := placement.ValidationRejectionReason(err)

	// Permanent error — terminate the lease
	isPending := lease.State == billingtypes.LEASE_STATE_PENDING
	if isPending {
		slog.Warn("reconcile: permanent provisioning error, rejecting pending lease",
			"lease_uuid", leaseUUID,
			"tenant", lease.Tenant,
			"reason", reason,
			"error", err,
		)
		if rejectErr := r.rejectLease(ctx, action, reason); rejectErr != nil {
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
		if closeErr := r.closeLease(ctx, action, reason); closeErr != nil {
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
	handleStartResult := func(result provisionStartResult) {
		err := result.Err()
		if errors.Is(err, errPlacementSnapshotStale) || errors.Is(err, errTrackerSnapshotStale) {
			deferred.Add(1)
			metrics.ReconcilerDeferredLeasesTotal.Inc()
			slog.Debug("reconcile: deferring lease whose placement changed before the provision write-ahead fence",
				"lease_uuid", leaseUUID,
			)
			return
		}
		r.handleProvisionResult(ctx, result, leaseUUID, lease, authority.action, &hadError)
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

	// ObserveLiveAction acquired the exact lease claim after rejecting every
	// operation that crossed the sweep boundary, so a competing in-flight
	// lifecycle operation is unrepresentable in this scope.
	inFlight := false
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
		var result provisionStartResult
		if plan.withPayload {
			result = r.startProvisioningWithPayload(ctx, lease, authority)
		} else {
			result = r.startProvisioning(ctx, lease, authority)
		}
		if result.Err() != nil {
			handleStartResult(result)
		} else if lease.State == billingtypes.LEASE_STATE_PENDING {
			provisioned.Add(1)
		}

	case reconcileActionAcknowledge:
		if err := r.acknowledgeLease(ctx, authority.action); err != nil {
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
		if err := r.rejectLease(ctx, authority.action, plan.reason); err != nil {
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
		if err := r.closeLease(ctx, authority.action, fmt.Sprintf("provision failed %d times", provision.FailCount)); err != nil {
			slog.Error("reconcile: failed to close exhausted lease",
				"lease_uuid", leaseUUID,
				"error", err,
			)
			hadError = true
			return
		}
		if err := r.coordinator.DeprovisionObserved(ctx, authority.action); err != nil {
			slog.Warn("reconcile: failed to deprovision after closing exhausted lease",
				"lease_uuid", leaseUUID,
				"error", err,
			)
		}

	case reconcileActionReconcileCustomDomain:
		if err := r.coordinator.ReconcileObservedCustomDomain(ctx, authority.action); err != nil {
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
	projected *placement.ProjectedReconciliationSweep,
	orphans, leaseErrors *atomic.Int32,
) {
	// Check context before doing any work to respect cancellation
	if ctx.Err() != nil {
		return
	}

	// The composite sweep derives the backend from its sealed provision row,
	// claims the exact Registry boundary, and performs the positive terminal
	// chain reread before returning any executable orphan authority.
	action, disposition, err := projected.ObserveTerminalOrphan(ctx, leaseUUID)
	if err != nil {
		metrics.ReconcilerCleanupSkipsTotal.
			WithLabelValues(metrics.CleanupPassOrphan, metrics.CleanupSkipChainError).Inc()
		slog.Error("reconcile: failed to confirm orphan terminality",
			"lease_uuid", leaseUUID, "error", err)
		leaseErrors.Add(1)
		return
	}
	var cleanupReason string
	switch disposition {
	case placement.ReconciliationObservationChainNotFound:
		cleanupReason = metrics.CleanupSkipChainUnknown
	case placement.ReconciliationObservationChainLive:
		cleanupReason = metrics.CleanupSkipChainLive
	case placement.ReconciliationObservationChainUnknownState:
		cleanupReason = metrics.CleanupSkipChainUnknownState
	}
	if cleanupReason != "" {
		metrics.ReconcilerCleanupSkipsTotal.
			WithLabelValues(metrics.CleanupPassOrphan, cleanupReason).Inc()
		if disposition == placement.ReconciliationObservationChainNotFound {
			slog.Warn("reconcile: orphan has no chain record; MANUAL CLEANUP MAY BE REQUIRED",
				"lease_uuid", leaseUUID, "reason", cleanupReason)
		} else {
			slog.Info("reconcile: skipping orphan without positive terminal chain authority",
				"lease_uuid", leaseUUID, "reason", cleanupReason)
		}
		return
	}
	if disposition != placement.ReconciliationObservationReady || !action.Valid() {
		if disposition == placement.ReconciliationObservationOperationBusy {
			metrics.ReconcilerInflightSkipsTotal.Inc()
		}
		slog.Debug("reconcile: skipping orphan without composite terminal authority",
			"lease_uuid", leaseUUID, "observation_disposition", disposition,
		)
		return
	}
	defer r.coordinator.ReleaseOrphanAction(action)
	orphans.Add(1)
	slog.Warn("reconcile: orphan provision found, deprovisioning",
		"lease_uuid", leaseUUID,
		"backend", action.BackendName(),
	)

	if err := r.coordinator.DeprovisionOrphan(ctx, action); err != nil {
		slog.Error("reconcile: failed to deprovision orphan",
			"lease_uuid", leaseUUID,
			"backend", action.BackendName(),
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
			// see errPayloadNotAvailable and remain unable to reconstruct the
			// tenant's exact request.
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
//   - ReconciliationSweep.Project minted opaque absence proof for this exact Store,
//     coordinator, inventory session, lease, and record revision. Minting
//     requires every durable candidate owner to have answered both /provisions
//     and /retentions and the lease to be absent from the raw positive union;
//   - the lease is not in-flight (a just-Set placement the backends haven't
//     reported yet — the exact race the old additive-only code avoided);
//   - the joined coordinator validates the exact Registry lease claim, then an
//     exact bounded GetLease positively reports one of CLOSED, REJECTED, or
//     EXPIRED before the Store atomically consumes the still-current proof.
//     Snapshot absence, nil, mismatched UUID, query failure, and unknown future
//     states are never terminal evidence.
//
// Returns the number of placements pruned.
func (r *Reconciler) cleanupOrphanedPlacements(
	ctx context.Context,
	chainLeases map[string]billingtypes.Lease,
	projected *placement.ProjectedReconciliationSweep,
	now time.Time,
) int {
	return r.placementPruner.cleanup(
		ctx, chainLeases, projected, now, r.placementCleanupBudget,
	)
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
