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
	"runtime/debug"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/metrics"
	"github.com/manifest-network/fred/internal/provisioner/payload"
	"github.com/manifest-network/fred/internal/provisioner/placement"
)

// Default concurrency limits for reconciliation.
const (
	// DefaultReconcileWorkers is the default number of concurrent workers for
	// processing leases and orphans during reconciliation.
	DefaultReconcileWorkers = 10
)

// chainConfirmTimeout bounds ONE per-candidate lease lookup in confirmTerminal.
//
// The reconcile context is the process lifetime — Start passes it straight
// through to every sweep — and neither the chain client nor gRPC imposes a
// per-RPC deadline of its own, so without this an unanswered Lease query stalls
// the sweep that issued it while ReconcileAll's CAS flag makes every later tick
// a no-op. The fail-open path this guard exists to reach (count chain_error,
// keep the state, retry next sweep) is only reachable if the call returns.
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

// errPayloadNotAvailable indicates the payload required for provisioning is
// not in the store. This is a permanent failure — the lease cannot be
// re-provisioned and should be closed.
var errPayloadNotAvailable = errors.New("payload not available")

// Note: InFlightTracker and ReconcilerTracker interfaces are defined in tracker.go

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
	providerUUID    string
	callbackBaseURL string
	chainClient     ReconcilerChainClient
	acknowledger    Acknowledger // Routes acks through the batcher for parallel signing
	backendRouter   BackendRouter
	tracker         ReconcilerTracker // For tracking in-flight provisions (shared state with event-driven path)
	placementStore  PlacementStore    // Optional placement store for backend routing

	interval               time.Duration
	maxWorkers             int         // Maximum concurrent workers for lease processing
	maxReprovisionAttempts int         // Max re-provision attempts before rejecting
	reconciling            atomic.Bool // Non-blocking flag to prevent concurrent reconciliation
	placementSweepSeen     atomic.Bool // True after this process durably synced one complete provision sweep.
	// placementAbsenceUntrusted narrows exceptions to the process-wide absence
	// proof. A positive observation excluded from placement sync because its
	// outbound operation straddled the inventory boundary must remain untrusted
	// for that lease until a later durable observation or complete inventory
	// settles it. ReconcileAll is serialized, and workers only read this map after
	// the sweep has finished updating it.
	placementAbsenceUntrusted map[string]struct{}
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
}

// NewReconciler creates a new reconciler.
// The acknowledger (required) routes ack operations through the batcher for parallel signing.
// The tracker parameter is optional only when placement tracking is disabled.
// A placement store and tracker form one causal barrier around backend calls;
// accepting only half would let reconciliation clear an attempt whose call is
// still crossing the process boundary.
// The placementStore parameter is optional - if nil, placement tracking is disabled.
func NewReconciler(cfg ReconcilerConfig, chainClient ReconcilerChainClient, acknowledger Acknowledger, backendRouter BackendRouter, tracker ReconcilerTracker, placementStore PlacementStore) (*Reconciler, error) {
	if chainClient == nil {
		return nil, errors.New("chain client is required")
	}
	if acknowledger == nil {
		return nil, errors.New("acknowledger is required")
	}
	if backendRouter == nil {
		return nil, errors.New("backend router is required")
	}
	if placementStore != nil && tracker == nil {
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

	return &Reconciler{
		providerUUID:              cfg.ProviderUUID,
		callbackBaseURL:           cfg.CallbackBaseURL,
		chainClient:               chainClient,
		acknowledger:              acknowledger,
		backendRouter:             backendRouter,
		tracker:                   tracker,
		placementStore:            placementStore,
		interval:                  interval,
		maxWorkers:                maxWorkers,
		maxReprovisionAttempts:    maxReprovision,
		placementAbsenceUntrusted: make(map[string]struct{}),
		ambiguousPlacements:       make(map[string][]string),
	}, nil
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
// DEFERRED — no row is applied to them and the next sweep tries again — because
// acting on a lease fred cannot see risks re-provisioning it onto a healthy peer
// and laying an empty volume over live data (ENG-356). See deferLease.
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

	// Capture both operation generations before the first chain/backend read.
	// Every lifecycle decision below must be based on state no older than this
	// boundary; an operation that starts or finishes afterward is deferred by its
	// placement revision, tracker tombstone, or live action claim.
	var placementSnapshotRevision uint64
	if r.placementStore != nil {
		placementSnapshotRevision = r.placementStore.SnapshotRevision()
	}
	inFlightAtSnapshot := make(map[string]struct{})
	var trackerSnapshotRevision uint64
	if r.tracker != nil {
		trackerSnapshotRevision = r.tracker.SnapshotMutationRevision()
		for _, leaseUUID := range r.tracker.GetInFlightLeases() {
			inFlightAtSnapshot[leaseUUID] = struct{}{}
		}
	}

	// 1. Get all leases from chain (pending and active)
	pendingLeases, err := r.chainClient.GetPendingLeases(ctx, r.providerUUID)
	if err != nil {
		return fmt.Errorf("failed to get pending leases: %w", err)
	}

	// Check for cancellation between chain queries
	if err := ctx.Err(); err != nil {
		return err
	}

	activeLeases, err := r.chainClient.GetActiveLeasesByProvider(ctx, r.providerUUID)
	if err != nil {
		return fmt.Errorf("failed to get active leases: %w", err)
	}

	// Build a map of all chain leases
	chainLeases := make(map[string]billingtypes.Lease)
	for _, lease := range pendingLeases {
		chainLeases[lease.Uuid] = lease
	}
	for _, lease := range activeLeases {
		chainLeases[lease.Uuid] = lease
	}

	slog.Info("fetched chain leases",
		"pending", len(pendingLeases),
		"active", len(activeLeases),
	)

	// 2. Get provisions from ALL backends (in parallel). This never fails the
	// sweep: a backend that does not answer is recorded as unanswered and its
	// leases are deferred below, rather than aborting reconciliation for the
	// whole fleet (ENG-356).
	snapshot := r.fetchFleetSnapshot(ctx)
	allProvisions := snapshot.provisions

	if snapshot.complete {
		metrics.ReconcilerSweepComplete.Set(1)
		slog.Info("fetched backend provisions", "total", len(allProvisions))
	} else {
		metrics.ReconcilerSweepComplete.Set(0)
		slog.Warn("reconciling with an incomplete view of the fleet; leases on unanswered backends will be deferred",
			"total", len(allProvisions),
			"unanswered", snapshot.unansweredBackends(),
		)
	}

	// Retained leases also pin a backend (restore affinity, ENG-333). Fetch them
	// only when placement tracking is enabled: the results feed solely the
	// placement sync and the pruner below, both of which no-op without a
	// placement store. Skipping avoids pointless per-backend /retentions calls and
	// log noise on placement-disabled deployments.
	var allRetentions map[string]string
	var retentionsAnswered answeredSet
	var retentionsReportedByBackend map[string]map[string]struct{}
	if r.placementStore != nil {
		allRetentions, retentionsAnswered, retentionsReportedByBackend = r.fetchAllRetentions(ctx)
		slog.Info("fetched backend retentions",
			"total", len(allRetentions),
			"complete", retentionsAnswered.complete(),
			"unanswered", retentionsAnswered.unanswered(),
		)
	}
	currentAmbiguities := ambiguousReportedOwners(snapshot.reportedByBackend, retentionsReportedByBackend)
	inventoryComplete := snapshot.complete && (r.placementStore == nil || retentionsAnswered.complete())
	// Merge new conflicts immediately, but do not resolve an older quarantine
	// until the corresponding durable placement sync has committed below.
	ambiguousOwners := r.updatePlacementAmbiguities(currentAmbiguities, false)
	if r.placementStore == nil && inventoryComplete {
		ambiguousOwners = r.updatePlacementAmbiguities(currentAmbiguities, true)
	}

	// Sync placements from actual backend state (handles cold start and drift).
	// NOTE: This sync only adds/updates. Pruning of orphaned placements is done
	// separately and gated — see cleanupOrphanedPlacements below. A naive prune
	// HERE would be unsafe because a concurrent StartProvisioning may have just
	// Set a placement that backends haven't reported yet; that race is exactly
	// why the pruner gates on chain-terminal + absent-from-all-backends +
	// not-in-flight rather than pruning during this additive sync.
	placementSyncOK := false
	var placementRecords map[string]placement.Placement
	sameSweepPlacementRevisions := make(map[string]uint64)
	if r.placementStore != nil {
		syncOK := true
		if err := r.placementStore.SetConflictsIfNotNewer(currentAmbiguities, placementSnapshotRevision); err != nil {
			syncOK = false
			slog.Warn("failed to persist ambiguous placement quarantine", "error", err)
		}

		supersededAttempts := make(map[string]string)
		authoritativelyAbsent := func(leaseUUID, backendName string) bool {
			if !inventoryComplete || !snapshot.answered.heard(backendName) ||
				!retentionsAnswered.heard(backendName) {
				return false
			}
			if _, present := snapshot.reportedByBackend[backendName][leaseUUID]; present {
				return false
			}
			if _, retained := retentionsReportedByBackend[backendName][leaseUUID]; retained {
				return false
			}
			return true
		}
		conflictCandidatesAccounted := func(p placement.Placement) bool {
			if !p.Conflict || p.ConflictOwnersUnknown || len(p.ConflictBackends) < 2 {
				return false
			}
			for _, backendName := range p.ConflictBackends {
				if !snapshot.answered.heard(backendName) || !retentionsAnswered.heard(backendName) {
					return false
				}
			}
			return true
		}

		// A positive report may establish an absent placement, but it must not
		// silently move an existing pin while that pin's backend is unavailable.
		// Moving A -> B is safe only when this complete snapshot positively reports
		// B and proves A absent from both provision and retention inventories.
		// An observation on another target can supersede an attempt only after the
		// attempted backend is authoritatively absent.
		acceptObservation := func(leaseUUID, backendName string, reporterFresh bool) bool {
			if backendName == "" {
				return false
			}
			p := r.placementStore.Lookup(leaseUUID)
			if !reporterFresh {
				// A cached positive after RefreshState failed is not fresh enough to
				// settle an Attempt or replace another known owner. It is still useful
				// conservative affinity when no durable fact conflicts with it: keeping
				// that pin prevents a later degraded sweep from routing elsewhere.
				return p.State() == placement.StateAbsent ||
					(p.Backend == backendName && p.Attempt == "" && !p.Conflict)
			}
			if p.Conflict {
				// A complete view of only the currently configured fleet is not enough
				// when a durable conflict names a former backend. Every recorded
				// candidate must still be configured and must have freshly answered both
				// inventories before one current report can become the unique owner.
				if !inventoryComplete || !conflictCandidatesAccounted(p) {
					return false
				}
			}
			if p.Attempt != "" && p.Attempt != backendName {
				if !authoritativelyAbsent(leaseUUID, p.Attempt) {
					return false
				}
				// SetBatch preserves a mismatched Attempt by design. Remember this
				// complete-snapshot proof so it can clear that exact attempt, by CAS,
				// only after the positive replacement owner commits below.
				supersededAttempts[leaseUUID] = p.Attempt
			}
			if p.Backend == "" || p.Backend == backendName {
				return true
			}
			return authoritativelyAbsent(leaseUUID, p.Backend)
		}

		placements := make(map[string]string, len(allProvisions)+len(allRetentions))
		placementObservationsExcluded := make(map[string]struct{})
		for leaseUUID, provision := range allProvisions {
			if _, ambiguous := currentAmbiguities[leaseUUID]; ambiguous {
				continue
			}
			if _, quarantined := ambiguousOwners[leaseUUID]; quarantined && !inventoryComplete {
				continue
			}
			if acceptObservation(leaseUUID, provision.BackendName,
				snapshot.answered.heard(provision.BackendName)) {
				placements[leaseUUID] = provision.BackendName
			}
		}
		// Retained leases pin their backend too — but only on a COMPLETE sweep.
		//
		// A retention proves a past deprovision on that backend, not present
		// ownership. This map is SetBatch'd BEFORE the per-lease loop reads it
		// back through placementFor, so a retention-derived record would
		// manufacture the very evidence deferLease uses to decide it is safe to
		// proceed — and would then aim both provision
		// (routeForProvisionHonoringPlacement) and deprovision (the
		// orchestrator's positive resolution) at that backend. Durably, in
		// bbolt, outliving the outage that produced it.
		//
		// Gating on "no existing record" instead would be backwards: creating a
		// record is precisely the DEFER→PROCEED flip, while overwriting an
		// existing one is the comparatively safe half.
		//
		// Skipping costs only a delayed backfill of a derived index. This sync
		// is additive and runs every sweep, retentions outlive the outage, and
		// placement already survives close by design — so the next complete
		// sweep repaves whatever was missed.
		if inventoryComplete {
			// Active provisions take precedence (if a stale retention races a
			// fresh provision, the provision wins).
			for leaseUUID, backendName := range allRetentions {
				if _, ambiguous := currentAmbiguities[leaseUUID]; ambiguous {
					continue
				}
				// Historical quarantines are deliberately not skipped here merely
				// because this is the complete-inventory branch. currentAmbiguities
				// rejects a still-duplicated retention above, while acceptObservation
				// requires complete evidence from every durable conflict candidate
				// before resolving an older quarantine to one retained owner.
				if _, isActive := placements[leaseUUID]; isActive {
					continue
				}
				if acceptObservation(leaseUUID, backendName,
					retentionsAnswered.heard(backendName)) {
					placements[leaseUUID] = backendName
				}
			}
		} else if len(allRetentions) > 0 {
			slog.Debug("reconcile: skipping retention-derived placement backfill, fleet view is incomplete",
				"retentions", len(allRetentions))
		}
		// A call already active when inventory began owns its placement
		// transition. Even a matching positive may describe an older incarnation
		// of the lease, so this snapshot must not clear or overwrite its attempt.
		// Calls starting after the boundary are excluded independently by revision.
		for leaseUUID := range inFlightAtSnapshot {
			if _, observed := placements[leaseUUID]; observed {
				placementObservationsExcluded[leaseUUID] = struct{}{}
			}
			delete(placements, leaseUUID)
		}
		appliedRevisions, err := r.placementStore.SetBatchIfNotNewer(placements, placementSnapshotRevision)
		if err != nil {
			syncOK = false
			slog.Warn("failed to sync placements from backend state", "error", err)
		} else {
			maps.Copy(sameSweepPlacementRevisions, appliedRevisions)
			// A complete snapshot can simultaneously disprove an old attempted
			// owner A and positively identify B. Commit B first, then clear A from
			// the resulting record with its new exact revision. Clearing first would
			// create a recordless window, while using the pre-snapshot revision after
			// SetBatch would necessarily fail and leave the lease gated forever.
			for leaseUUID, attemptedBackend := range supersededAttempts {
				observedBackend, included := placements[leaseUUID]
				if !included {
					continue
				}
				p := r.placementStore.Lookup(leaseUUID)
				if p.Backend != observedBackend || p.Attempt != attemptedBackend {
					continue
				}
				if p.Revision() > placementSnapshotRevision &&
					sameSweepPlacementRevisions[leaseUUID] != p.Revision() {
					// This record was not written by the inventory transaction above.
					// Preserve the newer operation-owned transition for a later sweep.
					continue
				}
				cleared, err := r.placementStore.ClearAttemptIfRevision(
					leaseUUID, attemptedBackend, p.Revision(),
				)
				switch {
				case err != nil:
					syncOK = false
					slog.Warn("failed to settle superseded placement attempt",
						"lease_uuid", leaseUUID,
						"attempted_backend", attemptedBackend,
						"observed_backend", observedBackend,
						"error", err,
					)
				case cleared:
					settled := r.placementStore.Lookup(leaseUUID)
					sameSweepPlacementRevisions[leaseUUID] = settled.Revision()
				default:
					slog.Debug("superseded placement attempt changed during settlement; preserving newer record",
						"lease_uuid", leaseUUID,
						"attempted_backend", attemptedBackend,
						"observed_backend", observedBackend,
					)
				}
			}
		}
		if inventoryComplete {
			resolvedAbsent := make(map[string]struct{})
			for leaseUUID, p := range r.placementStore.List() {
				if !p.Conflict {
					continue
				}
				if !conflictCandidatesAccounted(p) {
					continue
				}
				if _, wasInFlight := inFlightAtSnapshot[leaseUUID]; wasInFlight {
					continue
				}
				if _, stillAmbiguous := currentAmbiguities[leaseUUID]; stillAmbiguous {
					continue
				}
				if _, hasUniqueOwner := placements[leaseUUID]; !hasUniqueOwner {
					resolvedAbsent[leaseUUID] = struct{}{}
				}
			}
			if err := r.placementStore.ClearConflictsIfNotNewer(resolvedAbsent, placementSnapshotRevision); err != nil {
				syncOK = false
				slog.Warn("failed to clear resolved placement quarantine", "error", err)
			}
		}
		if syncOK {
			placementSyncOK = true
			ambiguousOwners = r.updatePlacementAmbiguities(currentAmbiguities, inventoryComplete)
			placementRecords = r.placementStore.List()
			if r.placementAbsenceUntrusted == nil {
				r.placementAbsenceUntrusted = make(map[string]struct{})
			}
			if inventoryComplete {
				// This snapshot accounts for every configured backend and therefore
				// settles every older per-lease exception. Operations that straddled
				// this very snapshot are re-added immediately below.
				clear(r.placementAbsenceUntrusted)
			} else {
				// A partial inventory cannot prove absence, but a durable non-absent
				// placement is enough to settle an older exception: future degraded
				// sweeps will use that record rather than bare absence.
				for leaseUUID := range r.placementAbsenceUntrusted {
					if _, excluded := placementObservationsExcluded[leaseUUID]; excluded {
						continue
					}
					if placementRecords[leaseUUID].State() != placement.StateAbsent {
						delete(r.placementAbsenceUntrusted, leaseUUID)
					}
				}
			}
			for leaseUUID := range placementObservationsExcluded {
				r.placementAbsenceUntrusted[leaseUUID] = struct{}{}
			}
			if inventoryComplete && len(ambiguousOwners) == 0 {
				r.placementSweepSeen.Store(true)
			}
		} else {
			// A write failure invalidates the process's earlier proof that record
			// absence is durable. Disarm until another complete, unambiguous sync;
			// otherwise a failed attempt-to-owner transition followed by a degraded
			// sweep could treat a missing record as authority to route elsewhere.
			r.placementSweepSeen.Store(false)
			placementRecords = r.placementStore.List()
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
			deferForSnapshotBoundary := func(reason string, generation uint64) {
				deferred.Add(1)
				metrics.ReconcilerDeferredLeasesTotal.Inc()
				slog.Debug("reconcile: deferring lease whose operation crossed the fleet snapshot boundary",
					"lease_uuid", leaseUUID,
					"reason", reason,
					"operation_generation", generation,
					"placement_revision", placementRecord.Revision(),
					"inventory_revision", placementSnapshotRevision,
				)
			}
			if _, wasInFlight := inFlightAtSnapshot[leaseUUID]; wasInFlight {
				if lease.State == billingtypes.LEASE_STATE_PENDING && isProvisioned &&
					provision.Status == backend.ProvisionStatusReady {
					metrics.ReconcilerInflightSkipsTotal.Inc()
				}
				deferForSnapshotBoundary("in_flight_at_inventory_start", 0)
				return nil
			}
			appliedRevision, appliedThisSweep := sameSweepPlacementRevisions[leaseUUID]
			if placementRecord.State() != placement.StateAbsent &&
				placementRecord.Revision() > placementSnapshotRevision &&
				(!appliedThisSweep || appliedRevision != placementRecord.Revision()) {
				deferForSnapshotBoundary("placement_changed_during_inventory", 0)
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
			attemptCleared := r.resolvePlacementAttempt(
				leaseUUID, placementRecord, snapshot, placementSnapshotRevision,
				inFlightAtSnapshot, retentionsAnswered, retentionsReportedByBackend,
			)
			if attemptCleared {
				placementRecord = r.placementFor(leaseUUID)
				// ClearAttemptIfRevision may delete an attempt-only record, whose
				// public Placement is the zero value and therefore cannot expose the
				// deletion revision. The store clock is nevertheless a valid upper
				// bound for this exact successful same-sweep CAS. The shared tracker
				// action fence below excludes any later operation on this lease, while
				// SetAttemptingIfNotNewer still rejects a placement mutation newer than
				// this point.
				placementCASRevision = r.placementStore.SnapshotRevision()
			}
			_, absenceUntrustedForLease := r.placementAbsenceUntrusted[leaseUUID]
			absenceTrusted := (r.placementSweepSeen.Load() && !absenceUntrustedForLease) || attemptCleared

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
			if deferLease(snapshot, isProvisioned, provision.BackendName, placementRecord, absenceTrusted) {
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
			// makes this worker's chain/backend inputs stale for this lease. The exact
			// tracker generation and placement revision comparisons keep the deferral
			// lease-local. The tracker action claim below is the final guard for chain
			// actions; provisioning atomically adds in-flight tracking while retaining
			// that claim through this worker's terminal decision, then the placement
			// cutoff CAS fences the backend side effect.
			currentPlacement := r.placementFor(leaseUUID)
			if currentPlacement.Revision() != placementRecord.Revision() ||
				currentPlacement.State() != placementRecord.State() {
				deferForSnapshotBoundary("placement_changed_after_sync", 0)
				return nil
			}
			if r.tracker != nil {
				if current, inFlight := r.tracker.GetInFlight(leaseUUID); inFlight {
					deferForSnapshotBoundary("operation_started_after_inventory", current.Generation)
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
					deferForSnapshotBoundary(reason, 0)
					return nil
				}
				defer r.tracker.ReleaseLeaseAction(leaseUUID)
			}

			r.processLease(gctx, leaseUUID, lease, provision, isProvisioned,
				placementCASRevision, trackerSnapshotRevision, &provisioned,
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
			r.processOrphan(ogctx, leaseUUID, provision, &orphans, &leaseErrors)
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
	case !snapshot.complete:
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
	)

	logFunc := slog.Info
	if leaseErrorCount > 0 || !snapshot.complete {
		logFunc = slog.Warn
	}
	logFunc("reconciliation complete",
		"provisioned", provisionedCount,
		"acknowledged", acknowledgedCount,
		"anomalies", anomaliesCount,
		"orphans", orphansCount,
		"errors", leaseErrorCount,
		"deferred", deferredCount,
		"sweep_complete", snapshot.complete,
		"placement_sync_ok", r.placementStore == nil || placementSyncOK,
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
	placementMaxRevision uint64,
	trackerMaxRevision uint64,
) error {
	return r.doStartProvisioning(ctx, lease, false, placementMaxRevision, trackerMaxRevision)
}

// startProvisioningWithPayload initiates provisioning for a lease that requires a payload.
// Returns errLeaseAlreadyInFlight if the lease is already being provisioned.
func (r *Reconciler) startProvisioningWithPayload(
	ctx context.Context,
	lease billingtypes.Lease,
	placementMaxRevision uint64,
	trackerMaxRevision uint64,
) error {
	return r.doStartProvisioning(ctx, lease, true, placementMaxRevision, trackerMaxRevision)
}

// doStartProvisioning is the common implementation for provisioning with or without payload.
func (r *Reconciler) doStartProvisioning(
	ctx context.Context,
	lease billingtypes.Lease,
	withPayload bool,
	placementMaxRevision uint64,
	trackerMaxRevision uint64,
) error {
	// Extract SKU for routing
	sku := ExtractRoutingSKU(&lease)

	// Route to appropriate backend, honoring existing placement for restored/placed leases (ENG-333)
	var inFlightByBackend map[string]int
	if r.tracker != nil {
		inFlightByBackend = r.tracker.InFlightCountsByBackend()
	}
	backendClient, err := routeForProvisionHonoringPlacement(ctx, r.backendRouter, r.placementStore, lease.Uuid, sku, inFlightByBackend)
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

	// Atomically track in manager's in-flight map if manager is available.
	// This prevents TOCTOU race between the reconciler and event-driven path:
	// both may try to provision the same lease concurrently.
	items := ExtractLeaseItems(&lease)
	var inFlightGeneration uint64
	if r.tracker != nil {
		var tracked, snapshotStale bool
		inFlightGeneration, tracked, snapshotStale = r.tracker.TryTrackInFlightWithGenerationIfNotNewer(
			lease.Uuid, lease.Tenant, items, backendClient.Name(), trackerMaxRevision,
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
		if r.tracker != nil {
			r.tracker.UntrackInFlightIfGeneration(lease.Uuid, inFlightGeneration)
		}
	}

	// Build provision request
	req := backend.ProvisionRequest{
		LeaseUUID:    lease.Uuid,
		Tenant:       lease.Tenant,
		ProviderUUID: r.providerUUID,
		Items:        items,
		CallbackURL:  BuildCallbackURLForGeneration(r.callbackBaseURL, inFlightGeneration),
	}

	// Get the payload from the store WITHOUT removing it yet.
	// We only delete after Provision() succeeds to allow retries.
	// Only include PayloadHash when we have the actual payload - this ensures
	// backends never receive a hash without the corresponding data.
	if withPayload && r.tracker != nil {
		// Read the payload and its recorded hash from ONE snapshot. Two reads
		// would let a concurrent /update commit between them and hand this
		// attempt the old payload with the new hash — which fails verification
		// below and deletes the update that was just persisted. Both paths are
		// live at once for an ACTIVE lease whose provision has failed: the
		// reconciler re-provisions it while the backend still accepts /update
		// for it, and nothing serializes the two.
		recordedHash, getErr := []byte(nil), error(nil)
		req.Payload, recordedHash, getErr = r.tracker.PayloadStore().GetWithHash(lease.Uuid)
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
				r.tracker.PayloadStore().Delete(lease.Uuid)
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

	// Persist the target immediately before the external side effect. All local
	// payload reads and validation above intentionally happen first so a local
	// preflight failure never manufactures an attempt for a request not sent.
	var (
		attemptRevision uint64
		attemptSet      = true
	)
	if r.placementStore != nil {
		attemptRevision, attemptSet, err = r.placementStore.SetAttemptingIfNotNewer(
			lease.Uuid, backendClient.Name(), placementMaxRevision,
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

	provisionErr := backendClient.Provision(ctx, req)
	outcome := classifyProvisionOutcome(provisionErr)
	if errors.Is(provisionErr, backend.ErrInsufficientResources) {
		metrics.BackendInsufficientResourcesTotal.WithLabelValues(backendClient.Name()).Inc()
	}
	settleErr := settleProvisionAttempt(
		r.placementStore, lease.Uuid, backendClient.Name(), attemptRevision, outcome,
	)
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
	case provisionOutcomeAlreadyExists:
		// A duplicate may not produce a callback. Positive inventory will drive
		// acknowledgement, so release the ordinary in-flight gate after recording
		// ownership (or leaving the conservative Attempt on write failure).
		untrack()
		return provisionErr
	case provisionOutcomeDefinitiveFailure, provisionOutcomeAmbiguous:
		// A definitive failure cleared Attempt when persistence succeeded. An
		// ambiguous result keeps it. Either way the durable record, rather than
		// the ephemeral tracker, gates the next call; releasing tracking lets the
		// reconciler consume a later authoritative inventory snapshot.
		untrack()
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
	if r.tracker != nil {
		if ps := r.tracker.PayloadStore(); ps != nil {
			ps.Delete(leaseUUID)
		}
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
	if r.placementStore == nil {
		return placement.Placement{}
	}
	return r.placementStore.Lookup(leaseUUID)
}

// resolvePlacementAttempt settles an attempt only with inventory that is known
// to post-date it. The store revision captured before fetchFleetSnapshot is the
// causal boundary: a newer attempt must survive this sweep even if its backend
// answered an earlier, lease-absent view. The revision-conditional clear closes
// the remaining race between this decision and a concurrent placement writer.
//
// It returns true only when this sweep durably proved and recorded per-lease
// absence. Callers may trust that fact even before the process-wide migration
// latch has armed.
func (r *Reconciler) resolvePlacementAttempt(
	leaseUUID string,
	p placement.Placement,
	snap fleetSnapshot,
	snapshotRevision uint64,
	inFlightAtSnapshot map[string]struct{},
	retentionsAnswered answeredSet,
	retentionsReportedByBackend map[string]map[string]struct{},
) bool {
	if r.placementStore == nil || p.Attempt == "" || p.Revision() > snapshotRevision {
		return false
	}
	// At startup, or after any failed/excluded placement sync, there is no durable
	// proof that a silent peer did not own this lease. Require a complete view
	// before clearing an attempt. Once a clean full sync has armed the trust latch,
	// the attempted backend's two fresh absence reports are sufficient.
	if !r.placementSweepSeen.Load() && (!snap.complete || !retentionsAnswered.complete()) {
		return false
	}
	if _, wasInFlight := inFlightAtSnapshot[leaseUUID]; wasInFlight {
		return false
	}
	if !snap.answered.heard(p.Attempt) {
		return false
	}
	// A lease can move from a provision into retained data without changing its
	// backend affinity. Negative proof therefore requires both independent list
	// endpoints from the attempted backend.
	if !retentionsAnswered.heard(p.Attempt) {
		return false
	}
	if reported := snap.reportedByBackend[p.Attempt]; reported != nil {
		if _, exists := reported[leaseUUID]; exists {
			// SetBatch normally confirmed this observation before the lease loop.
			// If a duplicate report or concurrent write left the attempt visible,
			// keeping it is safer than guessing which backend owns the lease.
			return false
		}
	}
	if reported := retentionsReportedByBackend[p.Attempt]; reported != nil {
		if _, exists := reported[leaseUUID]; exists {
			// Retained data is equally strong positive ownership evidence. The
			// flattened retention union may have been overwritten by a duplicate
			// report from another backend, so consult the per-backend membership.
			return false
		}
	}
	// A positive report from another backend is replacement evidence, not proof
	// that the lease is globally absent. The placement sync above must first
	// persist that owner and CAS-settle this attempt. If that write failed or was
	// skipped, retaining the attempt is what prevents a later degraded sweep from
	// treating the now-recordless lease as safe to route elsewhere.
	for backendName, reported := range snap.reportedByBackend {
		if backendName == p.Attempt {
			continue
		}
		if _, exists := reported[leaseUUID]; exists {
			return false
		}
	}
	for backendName, reported := range retentionsReportedByBackend {
		if backendName == p.Attempt {
			continue
		}
		if _, exists := reported[leaseUUID]; exists {
			return false
		}
	}
	// SetAttempting precedes the outbound call, so a fleet query can begin after
	// the write but finish before that call has taken effect. While the ordinary
	// in-flight entry exists, absence is therefore not yet a backend verdict.
	// Ambiguous synchronous results untrack after returning and leave the durable
	// Attempt as the retry gate; a later sweep can then settle it here.
	if r.tracker != nil && r.tracker.IsInFlight(leaseUUID) {
		return false
	}

	cleared, err := r.placementStore.ClearAttemptIfRevision(
		leaseUUID, p.Attempt, p.Revision(),
	)
	if err != nil {
		slog.Error("reconcile: failed to clear authoritatively absent placement attempt",
			"lease_uuid", leaseUUID,
			"backend", p.Attempt,
			"error", err,
		)
		return false
	}
	if !cleared {
		slog.Debug("reconcile: placement attempt changed while resolving; deferring",
			"lease_uuid", leaseUUID,
			"backend", p.Attempt,
		)
		return false
	}

	slog.Info("reconcile: cleared placement attempt after authoritative backend absence",
		"lease_uuid", leaseUUID,
		"backend", p.Attempt,
	)
	return true
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
// confirmed placement proceeds only when its own backend answered, even if
// every configured backend answered (the name may no longer be configured).
// A genuinely absent record proceeds on a complete sweep, after this process
// has previously completed a durable placement sync, or after this sweep
// authoritatively cleared that lease's sole attempt.
//
// Deferral is never destructive — it skips work rather than doing different
// work — so the cost of over-deferring is latency, while the cost of
// under-deferring is an empty volume laid over live tenant data.
func deferLease(snap fleetSnapshot, isProvisioned bool, reportedBackend string, p placement.Placement, absenceTrusted bool) bool {
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
		return !snap.answered.heard(p.Backend)
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

// confirmTerminal re-reads one lease from the chain and reports whether it is
// positively finished. A false return has already counted itself and logged;
// the caller just skips the lease.
//
// This runs on every candidate, not only on degraded sweeps: the staleness it
// guards against comes from the sweep's own non-atomic chain queries, and is
// identical on a sweep that saw every backend.
func (r *Reconciler) confirmTerminal(ctx context.Context, pass, leaseUUID string) bool {
	qctx, cancel := context.WithTimeout(ctx, chainConfirmTimeout)
	defer cancel()

	lease, err := r.chainClient.GetLease(qctx, leaseUUID)
	liveness, reason := classifyLease(lease, err)
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
//   - backend.ErrAlreadyProvisioned: skip (transient race with concurrent Deprovision)
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
		// Benign race: backend is concurrently Ready/Provisioning/Deprovisioning. Retry next cycle.
		slog.Debug("reconcile: backend reports already-provisioned, retry next cycle",
			"lease_uuid", leaseUUID,
		)
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
	placementMaxRevision uint64,
	trackerMaxRevision uint64,
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
				"inventory_revision", placementMaxRevision,
			)
			return
		}
		r.handleProvisionError(ctx, err, leaseUUID, lease, &hadError)
	}

	switch {
	case lease.State == billingtypes.LEASE_STATE_PENDING && !isProvisioned:
		// Check if lease requires a payload (has MetaHash)
		if len(lease.MetaHash) > 0 {
			// Lease needs a payload - check if we have one stored
			hasPayload := false
			if r.tracker != nil {
				var err error
				hasPayload, err = r.tracker.HasPayload(leaseUUID)
				if err != nil {
					slog.Error("reconcile: failed to check payload store",
						"lease_uuid", leaseUUID,
						"error", err,
					)
					hadError = true
				}
			}
			if hasPayload {
				// We have the payload - start provisioning with it
				if err := r.startProvisioningWithPayload(
					ctx, lease, placementMaxRevision, trackerMaxRevision,
				); err != nil {
					handleStartError(err)
				} else {
					provisioned.Add(1)
				}
			} else if !hadError {
				// No payload yet - wait for tenant to upload
				slog.Debug("reconcile: lease awaiting payload upload",
					"lease_uuid", leaseUUID,
					"tenant", lease.Tenant,
					"meta_hash_hex", fmt.Sprintf("%x", lease.MetaHash),
				)
			}
		} else {
			// No MetaHash - start provisioning immediately
			if err := r.startProvisioning(
				ctx, lease, placementMaxRevision, trackerMaxRevision,
			); err != nil {
				handleStartError(err)
			} else {
				provisioned.Add(1)
			}
		}

	case lease.State == billingtypes.LEASE_STATE_PENDING && isProvisioned && provision.Status == backend.ProvisionStatusReady:
		// Skip leases the main flow is actively processing — the success callback
		// will acknowledge via the same batcher. Without this guard the reconciler
		// races the callback-driven ack, wasting txs and triggering sequence churn.
		// Stuck-in-flight safety net: TimeoutChecker rejects the lease after the
		// configured CallbackTimeout expires if the main flow never untracks.
		if r.tracker != nil && r.tracker.IsInFlight(leaseUUID) {
			metrics.ReconcilerInflightSkipsTotal.Inc()
			slog.Debug("reconcile: skipping in-flight ready lease, main flow owns ack",
				"lease_uuid", leaseUUID,
			)
			break
		}
		// Provisioned but not acknowledged - acknowledge now
		if err := r.acknowledgeLease(ctx, leaseUUID); err != nil {
			slog.Error("reconcile: failed to acknowledge lease",
				"lease_uuid", leaseUUID,
				"error", err,
			)
			hadError = true
		} else {
			acknowledged.Add(1)
		}

	case lease.State == billingtypes.LEASE_STATE_PENDING && isProvisioned && provision.Status == backend.ProvisionStatusProvisioning:
		// Provisioning in progress - wait for callback
		slog.Debug("reconcile: lease provisioning in progress",
			"lease_uuid", leaseUUID,
		)

	case lease.State == billingtypes.LEASE_STATE_PENDING && isProvisioned && provision.Status == backend.ProvisionStatusFailed:
		// Provisioning failed - reject the lease so tenant's credit is released
		slog.Warn("reconcile: lease provisioning failed, rejecting",
			"lease_uuid", leaseUUID,
			"tenant", lease.Tenant,
		)
		if err := r.rejectLease(ctx, leaseUUID, "provisioning failed"); err != nil {
			slog.Error("reconcile: failed to reject lease",
				"lease_uuid", leaseUUID,
				"error", err,
			)
			hadError = true
		}

	case lease.State == billingtypes.LEASE_STATE_ACTIVE && !isProvisioned:
		// Anomaly: Lease is active but not provisioned
		// This shouldn't happen in normal operation
		slog.Warn("reconcile: anomaly - active lease not provisioned",
			"lease_uuid", leaseUUID,
			"tenant", lease.Tenant,
		)
		anomalies.Add(1)
		// Attempt to provision (with payload — Docker backend needs the manifest)
		if err := r.startProvisioningWithPayload(
			ctx, lease, placementMaxRevision, trackerMaxRevision,
		); err != nil {
			handleStartError(err)
		}

	case lease.State == billingtypes.LEASE_STATE_ACTIVE && isProvisioned && provision.Status == backend.ProvisionStatusFailed:
		// Anomaly: Lease is active but the container has crashed/exited.
		// This happens when a container dies after the success callback was sent
		// and the lease was acknowledged (e.g., OOM kill, runtime crash).
		anomalies.Add(1)

		if provision.FailCount >= r.maxReprovisionAttempts {
			// Too many failures — close the lease instead of retrying forever.
			// We use close (not reject) because the lease is ACTIVE.
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
			// Immediately release backend resources instead of waiting for
			// the next orphan-cleanup cycle.
			if b := r.backendRouter.GetBackendByName(provision.BackendName); b != nil {
				if err := b.Deprovision(ctx, leaseUUID); err != nil {
					slog.Warn("reconcile: failed to deprovision after closing exhausted lease",
						"lease_uuid", leaseUUID,
						"error", err,
					)
				}
			}
			return
		}

		slog.Warn("reconcile: anomaly - active lease has failed provision, re-provisioning",
			"lease_uuid", leaseUUID,
			"tenant", lease.Tenant,
			"backend", provision.BackendName,
			"fail_count", provision.FailCount,
			"max_attempts", r.maxReprovisionAttempts,
		)
		if err := r.startProvisioningWithPayload(
			ctx, lease, placementMaxRevision, trackerMaxRevision,
		); err != nil {
			handleStartError(err)
		}

	case lease.State == billingtypes.LEASE_STATE_ACTIVE && isProvisioned:
		// Healthy state — but reconcile per-LeaseItem custom_domain drift.
		// ReconcileCustomDomain is idempotent: it no-ops when nothing has
		// changed, so calling it every tick is cheap. Errors are logged
		// (so the next tick retries) and do not abort processing of other
		// leases.
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

	// In-flight guard (ENG-594): ReconcileAll snapshots chain leases BEFORE
	// fetching backend provisions. A lease created on-chain after that snapshot
	// but event-provisioned before the provisions fetch appears in provisions yet
	// not in chainLeases, so it looks like an orphan. Skip it while the main
	// provision flow still owns it — deprovisioning here would tear down a healthy
	// lease mid-provision. Mirrors the ack-skip branch and cleanupOrphanedPlacements,
	// which guard this same race; self-heals next sweep once the chain snapshot
	// includes the lease. TimeoutChecker is the safety net for a genuinely stuck
	// in-flight entry.
	if r.tracker != nil && r.tracker.IsInFlight(leaseUUID) {
		metrics.ReconcilerInflightSkipsTotal.Inc()
		slog.Debug("reconcile: skipping in-flight lease in orphan path, main flow owns it",
			"lease_uuid", leaseUUID,
		)
		return
	}

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
	if r.tracker == nil {
		return 0
	}
	payloadStore := r.tracker.PayloadStore()
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

// cleanupOrphanedPlacements is the SOLE pruner of the placement index (ENG-333).
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
) int {
	if r.placementStore == nil {
		return 0
	}

	cleaned := 0
	for leaseUUID, record := range r.placementStore.List() {
		if ctx.Err() != nil {
			break
		}
		// The backend inventories predate any record newer than the generation
		// captured at fetch start, so they cannot justify deleting it. Likewise, an
		// operation already in flight at that boundary may untrack after an
		// ambiguous response before this late cleanup pass; preserve it for a later
		// inventory that definitely started after the call.
		if record.Revision() > maxRevision {
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
		if r.tracker != nil {
			if _, inFlight := r.tracker.GetInFlight(leaseUUID); inFlight {
				continue
			}
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
		// Chain-terminal, absent from all backends, not in-flight → orphan.
		deleted, err := r.placementStore.DeleteIfRevision(leaseUUID, record.Revision())
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
