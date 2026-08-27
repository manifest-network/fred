package provisioner

import (
	"context"
	"log/slog"
	"maps"
	"runtime/debug"
	"slices"
	"sync"

	"golang.org/x/sync/errgroup"

	"github.com/manifest-network/fred/internal/metrics"
	"github.com/manifest-network/fred/internal/provisioner/placement"
)

// reconcileProjectionInput is the immutable boundary between fleet collection
// and durable placement projection. The inventory itself contains observations;
// the remaining fields fence those observations against concurrent operations.
type reconcileProjectionInput struct {
	inventory          reconcileInventory
	inventoryFence     placement.InventoryFence
	inFlightAtSnapshot map[string]struct{}
}

// reconcileProjectionResult is the placement authority produced for one
// reconciliation sweep. Its conservative zero value authorizes no work.
type reconcileProjectionResult struct {
	syncOK          bool
	records         map[string]placement.Placement
	ambiguousOwners map[string][]string
}

// projectPlacementInventory turns collected backend observations into one
// durable placement snapshot. It owns ambiguity carry-over, observation
// exclusions, atomic projection, readiness, and exclusion-marker retirement;
// it does not plan or execute any lease lifecycle action.
func (r *Reconciler) projectPlacementInventory(
	ctx context.Context,
	input reconcileProjectionInput,
) (reconcileProjectionResult, error) {
	inventory := input.inventory
	snapshot := inventory.fleet
	allProvisions := snapshot.provisions
	allRetentions := inventory.retentions
	retentionsAnswered := inventory.retentionsAnswered
	retentionsReportedByBackend := inventory.retentionsReportedByBackend
	currentAmbiguities := ambiguousReportedOwners(snapshot.reportedByBackend, retentionsReportedByBackend)
	inventoryComplete := inventory.complete()

	result := reconcileProjectionResult{
		// Merge new conflicts immediately, but do not resolve an older quarantine
		// until the corresponding durable placement sync has committed below.
		ambiguousOwners: r.updatePlacementAmbiguities(currentAmbiguities, false),
	}
	// Sync placements from actual backend state (handles cold start and drift).
	// NOTE: This sync only adds/updates. Pruning of orphaned placements is done
	// separately and gated — see cleanupOrphanedPlacements. A naive prune here
	// would race a concurrent StartProvisioning placement write.
	syncOK := true
	placementObservationsExcluded := make(map[string]map[string]struct{})
	freshPositiveObservations := make(map[string]string)
	excludeObservation := func(leaseUUID string, backendNames ...string) {
		if leaseUUID == "" {
			return
		}
		observed := placementObservationsExcluded[leaseUUID]
		if observed == nil {
			observed = make(map[string]struct{}, len(backendNames))
			placementObservationsExcluded[leaseUUID] = observed
		}
		for _, backendName := range backendNames {
			if backendName != "" {
				observed[backendName] = struct{}{}
			}
		}
	}
	// Inventory silence is not a remote execution barrier. A request can be
	// durably recorded here, time out ambiguously, and commit on the backend
	// after an arbitrarily later inventory response. Consequently, a positive
	// observation may confirm the exact recorded owner/attempt, but absence can
	// never clear an attempt or move a confirmed owner to another backend.
	// Contradictory positive evidence is accumulated into a durable quarantine.
	projectionConflicts := make(map[string][]string, len(currentAmbiguities))
	for leaseUUID, backendNames := range currentAmbiguities {
		projectionConflicts[leaseUUID] = slices.Clone(backendNames)
	}
	quarantineContradiction := func(
		leaseUUID, observedBackend string,
		p placement.Placement,
	) {
		candidateSet := make(map[string]struct{}, len(p.ConflictBackends)+3)
		for _, candidate := range append(
			slices.Clone(p.ConflictBackends), p.Backend, p.Attempt, observedBackend,
		) {
			if candidate != "" {
				candidateSet[candidate] = struct{}{}
			}
		}
		candidates := slices.Sorted(maps.Keys(candidateSet))
		if len(candidates) >= 2 {
			projectionConflicts[leaseUUID] = candidates
		}
	}
	// A positive report may establish an absent placement or confirm the exact
	// backend already recorded. It must never silently move a durable owner or
	// supersede an ambiguous attempt: inventory has no causal fence against a
	// delayed remote commit. Such contradictions are quarantined durably.
	acceptObservation := func(leaseUUID, backendName string, reporterFresh bool) bool {
		if backendName == "" {
			return false
		}
		p := r.placementAuthority.Lookup(leaseUUID)
		if !reporterFresh {
			// A cached positive after RefreshState failed is not fresh enough to
			// settle an Attempt or replace another known owner. It is still useful
			// conservative affinity when no durable fact conflicts with it: keeping
			// that pin prevents a later degraded sweep from routing elsewhere.
			return p.State() == placement.StateAbsent ||
				(p.Backend == backendName && p.Attempt == "" && !p.Conflict)
		}
		// An unreadable non-conflict record has lost the only durable pointer to
		// its historical owner. A single current report cannot prove that a silent
		// or removed backend has no second copy, so only operator repair may replace
		// this fail-closed record. Durable conflicts retain their candidate set.
		if p.State() == placement.StateUnusable && !p.Conflict {
			return false
		}
		if p.Conflict {
			// Positive evidence can enlarge or reaffirm a conflict, but inventory
			// silence cannot prove that any previously recorded candidate is gone.
			// Keep the durable union until an explicit operator repair or a future
			// receipt/fence protocol supplies causal non-execution proof.
			quarantineContradiction(leaseUUID, backendName, p)
			return false
		}
		if p.Attempt != "" && p.Attempt != backendName {
			quarantineContradiction(leaseUUID, backendName, p)
			return false
		}
		if p.Backend == "" || p.Backend == backendName {
			return true
		}
		quarantineContradiction(leaseUUID, backendName, p)
		return false
	}

	placements := make(map[string]string, len(allProvisions)+len(allRetentions))
	for leaseUUID, provision := range allProvisions {
		if _, ambiguous := projectionConflicts[leaseUUID]; ambiguous {
			continue
		}
		if _, quarantined := result.ambiguousOwners[leaseUUID]; quarantined && !inventoryComplete {
			continue
		}
		reporterFresh := snapshot.answered.heard(provision.BackendName)
		if acceptObservation(leaseUUID, provision.BackendName, reporterFresh) {
			placements[leaseUUID] = provision.BackendName
			if reporterFresh {
				freshPositiveObservations[leaseUUID] = provision.BackendName
			}
		}
	}
	// Retained leases pin their backend too — but only on a COMPLETE sweep.
	//
	// A retention proves a past deprovision on that backend, not present
	// ownership. This map is persisted before the per-lease loop reads it back,
	// so a retention-derived record would manufacture the evidence deferLease
	// uses to decide it is safe to proceed. Gating on a complete sweep preserves
	// the conservative behavior; a later complete sweep backfills anything
	// skipped during an outage.
	if inventoryComplete {
		// Active provisions take precedence if a stale retention races a fresh
		// provision.
		for leaseUUID, backendName := range allRetentions {
			if _, ambiguous := projectionConflicts[leaseUUID]; ambiguous {
				continue
			}
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

	// A call already active when inventory began owns its placement transition.
	// Even a matching positive may describe an older incarnation of the lease,
	// so this snapshot must not clear or overwrite its attempt.
	for leaseUUID := range input.inFlightAtSnapshot {
		if observedBackend, observed := placements[leaseUUID]; observed {
			p := r.placementAuthority.Lookup(leaseUUID)
			if p.State() != placement.StateConfirmed || p.Backend != observedBackend || p.Attempt != "" {
				excludeObservation(leaseUUID, observedBackend)
			}
		}
		delete(placements, leaseUUID)
	}
	// Refresh the process-local quarantine view with contradictions discovered
	// against durable records above. Silence never removes a quarantine; positive
	// observations can only reaffirm or enlarge its candidate union. Resolution
	// requires explicit operator action or future causally sufficient proof.
	result.ambiguousOwners = r.updatePlacementAmbiguities(projectionConflicts, false)
	projectionResult, err := r.placementAuthority.ProjectInventory(
		input.inventoryFence,
		placement.InventoryProjection{
			Complete:   inventoryComplete,
			Placements: placements,
			Conflicts:  projectionConflicts,
		},
	)
	for leaseUUID := range projectionResult.Fenced {
		switch {
		case placements[leaseUUID] != "":
			excludeObservation(leaseUUID, placements[leaseUUID])
		case len(projectionConflicts[leaseUUID]) > 0:
			excludeObservation(leaseUUID, projectionConflicts[leaseUUID]...)
		default:
			// The concrete Store currently fences only submitted placements or
			// conflicts. Keep this fail-closed arm because ReconcilerPlacement is an
			// injected capability and a future implementation may conservatively
			// fence an additional lease without attaching a backend name.
			excludeObservation(leaseUUID)
		}
	}
	if err != nil {
		syncOK = false
		for leaseUUID, backendName := range placements {
			excludeObservation(leaseUUID, backendName)
		}
		for leaseUUID, backendNames := range projectionConflicts {
			excludeObservation(leaseUUID, backendNames...)
		}
		slog.Warn("failed to atomically project backend inventory", "error", err)
	}

	if r.placementAbsenceUntrusted == nil {
		r.placementAbsenceUntrusted = make(map[string]map[string]struct{})
	}
	// Markers only gate lifecycle work for pending/active leases. Neither a
	// complete backend inventory nor absence from the chain's two filtered,
	// non-atomic lists proves terminality. Re-read every unlisted marker and prune
	// only a positive CLOSED/REJECTED/EXPIRED verdict.
	terminalMarkers := make(map[string]struct{})
	var terminalMarkersMu sync.Mutex
	markerChecks, markerCtx := errgroup.WithContext(ctx)
	markerChecks.SetLimit(r.maxWorkers)
	for leaseUUID := range r.placementAbsenceUntrusted {
		if _, live := inventory.chainLeases[leaseUUID]; live {
			continue
		}
		markerChecks.Go(func() (goErr error) {
			// GetLease ultimately crosses a gRPC boundary whose implementation is
			// outside the reconciler. Contain a panic to this one marker; the
			// marker remains fail-closed and a later sweep retries it.
			defer func() {
				if rec := recover(); rec != nil {
					slog.Error("reconciler placement marker check panic — recovering to keep fred alive",
						"lease_uuid", leaseUUID,
						"panic", rec,
						"stack", string(debug.Stack()),
					)
					metrics.ReconcilerPanicsTotal.WithLabelValues("check_placement_marker").Inc()
					goErr = nil
				}
			}()

			if r.placementMarkerLeaseTerminal(markerCtx, leaseUUID) {
				terminalMarkersMu.Lock()
				terminalMarkers[leaseUUID] = struct{}{}
				terminalMarkersMu.Unlock()
			}
			return nil
		})
	}
	if err := markerChecks.Wait(); err != nil {
		return reconcileProjectionResult{}, err
	}
	for leaseUUID := range terminalMarkers {
		delete(r.placementAbsenceUntrusted, leaseUUID)
	}
	if syncOK {
		result.syncOK = true
		result.ambiguousOwners = r.updatePlacementAmbiguities(projectionConflicts, inventoryComplete)
		result.records = r.placementAuthority.List()
		// Positive evidence is lease-local. During a partial fleet outage, a
		// fresh report can settle an older exclusion only when it is from the
		// exact sole backend retained by that marker and the durable record agrees.
		for leaseUUID, observedBackend := range freshPositiveObservations {
			excludedBackends, marked := r.placementAbsenceUntrusted[leaseUUID]
			if !marked || len(excludedBackends) != 1 {
				continue
			}
			if _, sameBackend := excludedBackends[observedBackend]; !sameBackend {
				continue
			}
			p := r.placementAuthority.Lookup(leaseUUID)
			if p.State() == placement.StateConfirmed && p.Backend == observedBackend && p.Attempt == "" {
				delete(r.placementAbsenceUntrusted, leaseUUID)
			}
		}
	} else {
		// A failed current projection authorizes no action derived from that
		// projection, but it does not erase a previously committed topology
		// baseline. Baseline admission is checked again atomically when an attempt
		// is inserted.
		result.records = r.placementAuthority.List()
	}
	// Current positive observations excluded by an operation boundary or
	// revision fence remain safety evidence even if another placement write in
	// the same synchronization failed.
	for leaseUUID, observedBackends := range placementObservationsExcluded {
		marker := r.placementAbsenceUntrusted[leaseUUID]
		if marker == nil {
			marker = make(map[string]struct{}, len(observedBackends))
			r.placementAbsenceUntrusted[leaseUUID] = marker
		}
		maps.Copy(marker, observedBackends)
	}

	return result, nil
}
