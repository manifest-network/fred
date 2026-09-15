package provisioner

import (
	"context"
	"log/slog"
	"maps"
	"runtime/debug"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"

	"github.com/manifest-network/fred/internal/metrics"
	"github.com/manifest-network/fred/internal/provisioner/placement"
)

// placementPruner owns the entire destructive proof. Its dependencies are
// joined once at construction and are never returned to the Reconciler.
type placementPruner struct {
	coordinator     *placement.ReconciliationCoordinator
	attemptRecovery *attemptRecoveryCoordinator
	interval        time.Duration
	maxWorkers      int

	cursorMu sync.Mutex
	cursor   string
}

func newPlacementPruner(
	coordinator *placement.ReconciliationCoordinator,
	attemptRecovery *attemptRecoveryCoordinator,
	interval time.Duration,
	maxWorkers int,
) *placementPruner {
	return &placementPruner{
		coordinator:     coordinator,
		attemptRecovery: attemptRecovery,
		interval:        interval,
		maxWorkers:      max(1, maxWorkers),
	}
}

// rotateCleanupCandidates gives every stable key a chance to run when a pass
// exhausts its shared budget. The cursor advances on scheduling, not success:
// an indefinitely stalled first candidate cannot starve the rest forever.
func (p *placementPruner) rotateCleanupCandidates(keys []string) []string {
	if len(keys) < 2 {
		return keys
	}
	p.cursorMu.Lock()
	defer p.cursorMu.Unlock()
	start := 0
	for index, key := range keys {
		if key > p.cursor {
			start = index
			break
		}
	}
	rotated := append(append(make([]string, 0, len(keys)), keys[start:]...), keys[:start]...)
	return rotated
}

func (p *placementPruner) advanceCursor(leaseUUID string) {
	p.cursorMu.Lock()
	p.cursor = leaseUUID
	p.cursorMu.Unlock()
}

// pruneTerminalCandidate owns the complete claim lifetime for one destructive
// decision. In particular, a panic from a chain or store adapter unwinds this
// frame before the worker's outer recovery runs, so ambiguity cannot strand the
// process-local lease claim and permanently block later lifecycle work.
func (p *placementPruner) pruneTerminalCandidate(
	ctx context.Context,
	leaseUUID string,
	projected *placement.ProjectedReconciliationSweep,
) bool {
	if projected == nil || !projected.Valid() {
		return false
	}
	result := projected.PruneTerminalAbsence(ctx, leaseUUID)
	if result.Deleted() {
		return true
	}
	metricReason := ""
	switch result.Disposition() {
	case placement.PruneDispositionChainError:
		metricReason = metrics.CleanupSkipChainError
	case placement.PruneDispositionChainUnknown:
		metricReason = metrics.CleanupSkipChainUnknown
		slog.Warn("reconcile: chain has no matching record of this lease — refusing to destroy its placement, MANUAL CLEANUP MAY BE REQUIRED",
			"lease_uuid", leaseUUID)
	case placement.PruneDispositionChainLive:
		metricReason = metrics.CleanupSkipChainLive
	case placement.PruneDispositionChainUnknownState:
		metricReason = metrics.CleanupSkipChainUnknownState
		slog.Warn("reconcile: lease is in a state this build does not recognize — refusing to destroy its placement",
			"lease_uuid", leaseUUID, "lease_state", result.LeaseState().String())
	case placement.PruneDispositionEvidenceStale:
		if result.Err() != nil {
			slog.Error("reconcile: failed to prune orphaned placement",
				"lease_uuid", leaseUUID, "error", result.Err())
		} else {
			slog.Debug("reconcile: projected absence changed while pruning; keeping record",
				"lease_uuid", leaseUUID)
		}
	default:
		slog.Debug("reconcile: projected absence cannot authorize pruning",
			"lease_uuid", leaseUUID, "error", result.Err())
	}
	if metricReason != "" {
		metrics.ReconcilerCleanupSkipsTotal.
			WithLabelValues(metrics.CleanupPassPlacement, metricReason).Inc()
	}
	return false
}

func (p *placementPruner) cleanup(
	ctx context.Context,
	chainLeases map[string]billingtypes.Lease,
	projected *placement.ProjectedReconciliationSweep,
	now time.Time,
	budget time.Duration,
) int {
	if budget <= 0 {
		budget = placementCleanupTimeout
	}
	cleanupCtx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()

	if projected == nil || !projected.Valid() {
		return 0
	}
	records := projected.Records()
	leaseUUIDs := slices.Sorted(maps.Keys(records))
	leaseUUIDs = p.rotateCleanupCandidates(leaseUUIDs)
	workers := max(1, p.maxWorkers)
	semaphore := make(chan struct{}, workers)
	var wait sync.WaitGroup
	var cleaned atomic.Int64

schedule:
	for _, leaseUUID := range leaseUUIDs {
		if cleanupCtx.Err() != nil {
			break
		}
		select {
		case semaphore <- struct{}{}:
		case <-cleanupCtx.Done():
			break schedule
		}
		p.advanceCursor(leaseUUID)
		record := records[leaseUUID]
		wait.Add(1)
		go func() {
			defer wait.Done()
			defer func() { <-semaphore }()
			// Each candidate is an independent failure domain. A chain/backend
			// adapter panic carries no terminal evidence for this record and must
			// neither crash providerd nor prevent healthy candidates from pruning.
			defer func() {
				if recovered := recover(); recovered != nil {
					metrics.ReconcilerPanicsTotal.WithLabelValues("placement_cleanup").Inc()
					slog.Error("reconciler placement cleanup panic; preserving exact record",
						"lease_uuid", leaseUUID,
						"panic", recovered,
						"stack", string(debug.Stack()),
					)
				}
			}()
			// A one-iteration loop lets the former sequential pass retain its
			// conservative `continue` exits while candidates run independently.
			for range 1 {
				if cleanupCtx.Err() != nil {
					break
				}
				// A live lease's ambiguous effect is handled by exact same-operation
				// redelivery in the main pass. For a lease absent from that PENDING/ACTIVE
				// snapshot, exact terminal convergence first proves its current chain state
				// and idempotently tears down only the durable candidate backend(s). Every
				// uncertainty preserves the Attempt; inventory silence alone never clears it.
				if record.Attempt != "" {
					if _, listedLive := chainLeases[leaseUUID]; !listedLive {
						metadata := record.AttemptMetadata()
						if metadata.Valid() {
							settled, settleErr := p.attemptRecovery.ConvergeTerminal(
								cleanupCtx, leaseUUID, projected,
							)
							if settled {
								continue
							}
							if settleErr != nil {
								slog.Warn("reconcile: terminal durable operation remains unresolved",
									"lease_uuid", leaseUUID,
									"backend", record.Backend,
									"attempt", record.Attempt,
									"operation_fingerprint", metadata.OperationID(),
									"operation_kind", metadata.Kind(),
									"error", settleErr,
								)
							}
						}
					}
					metrics.ReconcilerCleanupSkipsTotal.
						WithLabelValues(metrics.CleanupPassPlacement, metrics.CleanupSkipAttemptPending).Inc()
					slog.Debug("reconcile: keeping placement with unresolved backend attempt",
						"lease_uuid", leaseUUID,
						"backend", record.Backend,
						"attempt", record.Attempt,
						"operation_fingerprint", record.AttemptOperationID(),
					)
					continue
				}
				// An operation already in flight at the inventory boundary may finish
				// before this late cleanup pass; its typed snapshot tombstone keeps that
				// boundary visible even after the active record disappears.
				if projected.WasInFlight(leaseUUID) {
					slog.Debug("reconcile: keeping placement whose operation was in flight when inventory began",
						"lease_uuid", leaseUUID)
					continue
				}
				if !projected.HasPruneAbsence(leaseUUID) {
					metrics.ReconcilerCleanupSkipsTotal.
						WithLabelValues(metrics.CleanupPassPlacement, metrics.CleanupSkipBackendSilent).Inc()
					slog.Debug("reconcile: keeping placement without projection-minted absence proof",
						"lease_uuid", leaseUUID,
						"backend", record.Backend,
						"attempt", record.Attempt,
						"conflict_backends", record.ConflictBackends,
						"conflict_owners_unknown", record.ConflictOwnersUnknown,
						"state", record.State().String(),
					)
					continue
				}
				// A provision Set this placement moments ago; backends/chain may not
				// reflect it yet → keep (the documented additive-only race).
				if projected.OperationActiveNow(leaseUUID) {
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
				grace := 2 * p.interval
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
				// A terminal candidate, absent from all backends and not in-flight, is
				// still only a snapshot conclusion. The scoped helper owns the lifecycle
				// claim from inventory-boundary revalidation through the final CAS.
				if !p.pruneTerminalCandidate(
					cleanupCtx, leaseUUID, projected,
				) {
					continue
				}
				cleaned.Add(1)
				slog.Info("reconcile: pruned orphaned placement", "lease_uuid", leaseUUID)
			}
		}()
	}
	wait.Wait()
	return int(cleaned.Load())
}
