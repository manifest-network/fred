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
	inventory reconcileInventory
	sweep     *placement.ReconciliationSweep
}

// reconcileProjectionResult is the placement authority produced for one
// reconciliation sweep. Its conservative zero value authorizes no work.
type reconcileProjectionResult struct {
	syncOK          bool
	records         map[string]placement.Placement
	ambiguousOwners map[string][]string
	projected       *placement.ProjectedReconciliationSweep
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
	inventoryComplete := input.sweep != nil && input.sweep.InventoryComplete()

	result := reconcileProjectionResult{}
	// Sync placements from actual backend state (handles cold start and drift).
	// NOTE: This sync only adds/updates. Pruning of orphaned placements is done
	// separately and gated — see cleanupOrphanedPlacements. A naive prune here
	// would race a concurrent StartProvisioning placement write.
	syncOK := true
	// A rejected endpoint cannot contribute a placement payload, but its positive
	// lease membership remains safety evidence. Carry that evidence into the
	// lease-local exclusion map before any lifecycle planning can interpret the
	// now-incomplete union as absence.
	// Inventory silence is not a remote execution barrier. A request can be
	// durably recorded here, time out ambiguously, and commit on the backend
	// after an arbitrarily later inventory response. Consequently, only a
	// positive observation carrying the exact paired typed generation may
	// confirm an attempt; absence can never clear one or move a confirmed owner
	// to another backend.
	// Contradictory positive evidence is accumulated into a durable quarantine.
	projectionConflicts := make(map[string][]string, len(currentAmbiguities))
	durableCandidateUnion := func(leaseUUID string, backendNames ...string) []string {
		p := input.sweep.InitialRecord(leaseUUID)
		candidateSet := make(map[string]struct{}, len(backendNames)+len(p.ConflictBackends)+2)
		for _, candidate := range append(
			slices.Clone(p.ConflictBackends), p.Backend, p.Attempt,
		) {
			if candidate != "" {
				candidateSet[candidate] = struct{}{}
			}
		}
		for _, candidate := range backendNames {
			if candidate != "" {
				candidateSet[candidate] = struct{}{}
			}
		}
		return slices.Sorted(maps.Keys(candidateSet))
	}
	for leaseUUID, backendNames := range currentAmbiguities {
		projectionConflicts[leaseUUID] = durableCandidateUnion(leaseUUID, backendNames...)
	}
	// Rejected positive payloads cannot establish an owner, but their membership
	// fact must survive process restart. Project them into a distinct durable
	// quarantine. A current multi-reporter conflict already carries stronger
	// evidence and takes precedence.
	projectionUntrustedPositives := make(
		map[string][]string, len(inventory.untrustedPositiveObservations),
	)
	for leaseUUID, backendNames := range inventory.untrustedPositiveObservations {
		if _, conflicted := projectionConflicts[leaseUUID]; conflicted {
			continue
		}
		// Submit only reporters carrying rejected evidence. Store projection owns
		// the durable candidate union; labeling an existing owner as untrusted
		// would manufacture a fact absent from this sealed snapshot.
		projectionUntrustedPositives[leaseUUID] = slices.Sorted(maps.Keys(backendNames))
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
	// A positive report may establish routing for an absent placement. It can
	// consume an attempt only when the backend also reports its exact paired typed
	// lifecycle generation. It must never silently move a durable owner: inventory
	// has no causal fence against a delayed remote commit, so contradictions are
	// quarantined durably.
	acceptObservation := func(leaseUUID, backendName string, reporterFresh bool) bool {
		if backendName == "" {
			return false
		}
		p := input.sweep.InitialRecord(leaseUUID)
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
			if inventoryComplete && p.CanResolveUntrustedPositive(backendName) {
				return true
			}
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
		if _, untrusted := projectionUntrustedPositives[leaseUUID]; untrusted {
			continue
		}
		reporterFresh := snapshot.answered.heard(provision.BackendName)
		if acceptObservation(leaseUUID, provision.BackendName, reporterFresh) {
			placements[leaseUUID] = provision.BackendName
		}
	}
	// Retained leases pin their backend too — but only on a COMPLETE sweep.
	//
	// A retention proves a past deprovision on that backend, not present
	// ownership. This map is persisted before the per-lease loop reads it back,
	// so a retention-derived record would manufacture the evidence deferLease
	// uses to decide it is safe to proceed. Gating on a complete sweep preserves
	// the conservative behavior; a later complete sweep backfills anything
	// skipped during an outage. On an incomplete sweep ReconciliationSweep owns
	// the classification: it derives every retention reporter from its sealed
	// snapshot. The Store reaffirms an existing confirmed owner on the same
	// backend, while new or contradictory affinity becomes a lease-local durable
	// quarantine. The reconciler cannot omit that fact or promote a new owner.
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
	}

	// A call already active when inventory began owns its placement transition.
	// Even a matching positive may describe an older incarnation of the lease,
	// so this snapshot must not clear or overwrite its attempt.
	for leaseUUID := range placements {
		if !input.sweep.WasInFlight(leaseUUID) {
			continue
		}
		delete(placements, leaseUUID)
	}
	projected, err := input.sweep.Project(
		placement.ReconciliationProjection{
			Placements:         placements,
			Conflicts:          projectionConflicts,
			UntrustedPositives: projectionUntrustedPositives,
		},
	)
	if err != nil {
		syncOK = false
		slog.Warn("failed to atomically project backend inventory", "error", err)
	}

	// Markers only gate lifecycle work for pending/active leases. Neither a
	// complete backend inventory nor absence from the chain's two filtered,
	// non-atomic lists proves terminality. Re-read every unlisted marker and prune
	// only a positive CLOSED/REJECTED/EXPIRED verdict.
	terminalMarkers := make(map[string]struct{})
	var terminalMarkersMu sync.Mutex
	markerChecks, markerCtx := errgroup.WithContext(ctx)
	markerChecks.SetLimit(r.maxWorkers)
	for _, leaseUUID := range r.coordinator.AbsenceUntrustedLeaseUUIDs() {
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
		r.coordinator.RetireTerminalAbsenceUntrusted(leaseUUID)
	}
	if syncOK {
		result.syncOK = true
		result.projected = projected
		result.records = projected.Records()
		result.ambiguousOwners = placementConflicts(result.records)
	} else {
		// A failed current projection authorizes no action derived from that
		// projection, but it does not erase a previously committed topology
		// baseline. Baseline admission is checked again atomically when an attempt
		// is inserted.
		result.records = input.sweep.InitialRecords()
	}
	return result, nil
}
