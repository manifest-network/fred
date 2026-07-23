package provisioner

import (
	"cmp"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/metrics"
	"github.com/manifest-network/fred/internal/provisioner/payload"
)

// Default concurrency limits for reconciliation.
const (
	// DefaultReconcileWorkers is the default number of concurrent workers for
	// processing leases and orphans during reconciliation.
	DefaultReconcileWorkers = 10
)

// errLeaseAlreadyInFlight indicates the lease is already being provisioned.
// This is not a real error - the caller should not treat it as a failure.
var errLeaseAlreadyInFlight = errors.New("lease already in-flight")

// errPayloadNotAvailable indicates the payload required for provisioning is
// not in the store. This is a permanent failure — the lease cannot be
// re-provisioned and should be closed.
var errPayloadNotAvailable = errors.New("payload not available")

// Note: InFlightTracker and ReconcilerTracker interfaces are defined in tracker.go

// ReconcilerChainClient defines the chain operations needed by the reconciler.
type ReconcilerChainClient interface {
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
// The tracker parameter is optional - if nil, the reconciler will not coordinate with the event-driven path.
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
		providerUUID:           cfg.ProviderUUID,
		callbackBaseURL:        cfg.CallbackBaseURL,
		chainClient:            chainClient,
		acknowledger:           acknowledger,
		backendRouter:          backendRouter,
		tracker:                tracker,
		placementStore:         placementStore,
		interval:               interval,
		maxWorkers:             maxWorkers,
		maxReprovisionAttempts: maxReprovision,
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

	// 2. Get provisions from ALL backends (in parallel)
	allProvisions, err := r.fetchAllProvisions(ctx)
	if err != nil {
		return fmt.Errorf("failed to fetch provisions: %w", err)
	}

	slog.Info("fetched backend provisions", "total", len(allProvisions))

	// Retained leases also pin a backend (restore affinity, ENG-333). Fetch them
	// only when placement tracking is enabled: the results feed solely the
	// placement sync and the gated pruner below, both of which no-op without a
	// placement store. Skipping avoids pointless per-backend /retentions calls and
	// log noise on placement-disabled deployments.
	var allRetentions map[string]string
	var retentionsComplete bool
	if r.placementStore != nil {
		allRetentions, retentionsComplete = r.fetchAllRetentions(ctx)
		slog.Info("fetched backend retentions", "total", len(allRetentions), "complete", retentionsComplete)
	}

	// Sync placements from actual backend state (handles cold start and drift).
	// NOTE: This sync only adds/updates. Pruning of orphaned placements is done
	// separately and gated — see cleanupOrphanedPlacements below. A naive prune
	// HERE would be unsafe because a concurrent StartProvisioning may have just
	// Set a placement that backends haven't reported yet; that race is exactly
	// why the pruner gates on chain-terminal + absent-from-all-backends +
	// not-in-flight rather than pruning during this additive sync.
	if r.placementStore != nil && (len(allProvisions) > 0 || len(allRetentions) > 0) {
		placements := make(map[string]string, len(allProvisions)+len(allRetentions))
		for leaseUUID, provision := range allProvisions {
			if provision.BackendName != "" {
				placements[leaseUUID] = provision.BackendName
			}
		}
		// Retained leases pin their backend too. Active provisions take precedence
		// (if a stale retention races a fresh provision, the provision wins).
		for leaseUUID, backendName := range allRetentions {
			if _, isActive := placements[leaseUUID]; !isActive {
				placements[leaseUUID] = backendName
			}
		}
		if len(placements) > 0 {
			if err := r.placementStore.SetBatch(placements); err != nil {
				slog.Warn("failed to sync placements from backend state", "error", err)
			}
		}
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
	)

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(r.maxWorkers)

	for leaseUUID, lease := range chainLeases {
		provision, isProvisioned := allProvisions[leaseUUID]

		g.Go(func() error {
			// Recover any panic inside processLease so ONE bad lease
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
			r.processLease(gctx, leaseUUID, lease, provision, isProvisioned,
				&provisioned, &acknowledged, &anomalies, &leaseErrors)
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

	// Record outcome: partial if per-lease errors occurred, success otherwise
	if leaseErrorCount > 0 {
		metrics.ReconciliationTotal.WithLabelValues(metrics.OutcomePartial).Inc()
	} else {
		metrics.ReconciliationTotal.WithLabelValues(metrics.OutcomeSuccess).Inc()
		metrics.ReconcilerLastSuccessTimestamp.SetToCurrentTime()
	}

	// 5. Clean up orphaned payloads (payloads for leases that are no longer pending)
	orphanedPayloads := r.cleanupOrphanedPayloads(ctx, chainLeases)

	// 6. Prune orphaned placements (ENG-333 — sole pruner; see cleanupOrphanedPlacements).
	// Per-lease deletions are logged inside the method; the aggregate count is
	// surfaced via the reconciliation-complete summary below.
	prunedPlacements := r.cleanupOrphanedPlacements(ctx, chainLeases, backendLeases, retentionsComplete, startTime)

	logFunc := slog.Info
	if leaseErrorCount > 0 {
		logFunc = slog.Warn
	}
	logFunc("reconciliation complete",
		"provisioned", provisionedCount,
		"acknowledged", acknowledgedCount,
		"anomalies", anomaliesCount,
		"orphans", orphansCount,
		"errors", leaseErrorCount,
		"orphaned_payloads_cleaned", orphanedPayloads,
		"orphaned_placements_pruned", prunedPlacements,
	)

	return nil
}

// startProvisioning initiates provisioning for a lease without a payload.
// Returns errLeaseAlreadyInFlight if the lease is already being provisioned by
// the event-driven path (this is not a real error, just a signal to skip).
func (r *Reconciler) startProvisioning(ctx context.Context, lease billingtypes.Lease) error {
	return r.doStartProvisioning(ctx, lease, false)
}

// startProvisioningWithPayload initiates provisioning for a lease that requires a payload.
// Returns errLeaseAlreadyInFlight if the lease is already being provisioned.
func (r *Reconciler) startProvisioningWithPayload(ctx context.Context, lease billingtypes.Lease) error {
	return r.doStartProvisioning(ctx, lease, true)
}

// doStartProvisioning is the common implementation for provisioning with or without payload.
func (r *Reconciler) doStartProvisioning(ctx context.Context, lease billingtypes.Lease, withPayload bool) error {
	// Extract SKU for routing
	sku := ExtractRoutingSKU(&lease)

	// Route to appropriate backend, honoring existing placement for restored/placed leases (ENG-333)
	var inFlightByBackend map[string]int
	if r.tracker != nil {
		inFlightByBackend = r.tracker.InFlightCountsByBackend()
	}
	backendClient := routeForProvisionHonoringPlacement(ctx, r.backendRouter, r.placementStore, lease.Uuid, sku, inFlightByBackend)
	if backendClient == nil {
		return fmt.Errorf("no backend available")
	}

	// Atomically track in manager's in-flight map if manager is available.
	// This prevents TOCTOU race between the reconciler and event-driven path:
	// both may try to provision the same lease concurrently.
	items := ExtractLeaseItems(&lease)
	if r.tracker != nil {
		if !r.tracker.TryTrackInFlight(lease.Uuid, lease.Tenant, items, backendClient.Name()) {
			metrics.ReconciliationConflictsTotal.Inc()
			return errLeaseAlreadyInFlight
		}
	}

	// Build provision request
	req := backend.ProvisionRequest{
		LeaseUUID:    lease.Uuid,
		Tenant:       lease.Tenant,
		ProviderUUID: r.providerUUID,
		Items:        items,
		CallbackURL:  BuildCallbackURL(r.callbackBaseURL),
	}

	// Get the payload from the store WITHOUT removing it yet.
	// We only delete after Provision() succeeds to allow retries.
	// Only include PayloadHash when we have the actual payload - this ensures
	// backends never receive a hash without the corresponding data.
	if withPayload && r.tracker != nil {
		var getErr error
		req.Payload, getErr = r.tracker.PayloadStore().Get(lease.Uuid)
		if getErr != nil {
			// Database error — do NOT treat as "payload missing".
			// Abort this provision attempt so a transient disk issue doesn't
			// cause us to close an active lease.
			r.tracker.UntrackInFlight(lease.Uuid)
			return fmt.Errorf("failed to read payload for lease %s: %w", lease.Uuid, getErr)
		}
		if req.Payload == nil && len(lease.MetaHash) > 0 {
			// Payload is required (lease has MetaHash) but not in the store.
			// This can happen if the payload DB was lost or fred restarted
			// without its data. We cannot re-provision without the manifest.
			r.tracker.UntrackInFlight(lease.Uuid)
			return fmt.Errorf("%w: lease %s", errPayloadNotAvailable, lease.Uuid)
		}
		if req.Payload != nil && len(lease.MetaHash) > 0 {
			// Re-verify payload hash before provisioning to catch any corruption.
			// The payload was validated on upload, but disk corruption could occur.
			if err := payload.VerifyHash(req.Payload, lease.MetaHash); err != nil {
				// Payload is corrupted - delete it and fail
				r.tracker.PayloadStore().Delete(lease.Uuid)
				r.tracker.UntrackInFlight(lease.Uuid)
				slog.Error("reconcile: payload hash mismatch - possible corruption",
					"lease_uuid", lease.Uuid,
					"error", err,
				)
				return err
			}
			req.PayloadHash = hex.EncodeToString(lease.MetaHash)
		}
	}

	err := backendClient.Provision(ctx, req)
	if err != nil {
		if errors.Is(err, backend.ErrInsufficientResources) {
			metrics.BackendInsufficientResourcesTotal.WithLabelValues(backendClient.Name()).Inc()
		}
		// Clean up in-flight on error.
		// Keep payload in store so next reconciliation can retry with it.
		if r.tracker != nil {
			r.tracker.UntrackInFlight(lease.Uuid)
		}
		return err
	}

	// Note: Payload is NOT deleted here. Cleanup happens later — when the
	// lease closes (HandleLeaseClosed) or when a PENDING-failure callback
	// rejects the lease and deletes the payload. Success and ACTIVE-failure
	// paths intentionally retain the payload so a subsequent re-provision
	// can reuse the same manifest. This also ensures the payload remains
	// available for retry if the backend fails or crashes before sending
	// a callback.

	// Record placement so read operations can find this lease's backend
	if r.placementStore != nil {
		if err := r.placementStore.Set(lease.Uuid, backendClient.Name()); err != nil {
			slog.Warn("failed to record placement",
				"lease_uuid", lease.Uuid,
				"backend", backendClient.Name(),
				"error", err,
			)
		}
	}

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

	// Eagerly delete placement for rejected PENDING leases: a PENDING lease was
	// never active long enough to have retained data on a backend, so there is
	// no restore-affinity window to protect (ENG-333).
	if r.placementStore != nil {
		r.placementStore.Delete(leaseUUID)
	}

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
// Exception: rejectLease — which handles PENDING leases that never had retained
// data — deletes the placement eagerly after calling cleanupTerminalLease.
func (r *Reconciler) cleanupTerminalLease(leaseUUID string) {
	if r.tracker != nil {
		if ps := r.tracker.PayloadStore(); ps != nil {
			ps.Delete(leaseUUID)
		}
	}
}

// fetchAllProvisions retrieves provisions from all backends in parallel.
// Returns an error if any backend fails to list provisions, because partial
// data would cause the reconciler to misidentify running containers as orphans
// or active leases as unprovisioned anomalies.
func (r *Reconciler) fetchAllProvisions(ctx context.Context) (map[string]backend.ProvisionInfo, error) {
	backends := r.backendRouter.Backends()

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(len(backends)) // Query all backends concurrently

	var mu sync.Mutex
	allProvisions := make(map[string]backend.ProvisionInfo)
	var fetchErrors []error

	var panicErrs []error
	for _, b := range backends {
		g.Go(func() (goErr error) {
			// Recover any panic from backend.RefreshState / ListProvisions
			// (or the HTTP/JSON path that implements them). We do NOT
			// surface as an errgroup error — returning non-nil from the
			// g.Go closure would trigger errgroup's fail-fast behavior
			// and cancel sibling backend fetches via gctx. Instead, we
			// append to panicErrs (protected by mu) which the caller
			// checks after g.Wait() and treats as a fetch failure on
			// par with a normal ListProvisions error. This mirrors the
			// behavior of the regular error path below (return nil
			// after recording into fetchErrors).
			defer func() {
				if rec := recover(); rec != nil {
					slog.Error("reconciler fetch panic — recovering to keep fred alive",
						"backend", b.Name(),
						"panic", rec,
						"stack", string(debug.Stack()),
					)
					metrics.ReconcilerPanicsTotal.WithLabelValues("fetch_provisions").Inc()
					mu.Lock()
					panicErrs = append(panicErrs, fmt.Errorf("backend %s panic: %v", b.Name(), rec))
					mu.Unlock()
					goErr = nil // Don't cancel siblings via errgroup; error surfaces via panicErrs.
				}
			}()
			// Ensure backend state is fresh before reading provisions.
			if err := b.RefreshState(gctx); err != nil {
				slog.Warn("failed to refresh backend state",
					"backend", b.Name(), "error", err,
				)
				// Continue — stale state is better than no state
			}

			provisions, err := b.ListProvisions(gctx)
			if err != nil {
				slog.Error("failed to list provisions from backend",
					"backend", b.Name(),
					"error", err,
				)
				mu.Lock()
				fetchErrors = append(fetchErrors, fmt.Errorf("backend %s: %w", b.Name(), err))
				mu.Unlock()
				return nil // Don't cancel other backends — collect all errors
			}

			mu.Lock()
			for _, p := range provisions {
				p.BackendName = b.Name()
				allProvisions[p.LeaseUUID] = p
			}
			mu.Unlock()

			slog.Debug("fetched backend provisions",
				"backend", b.Name(),
				"count", len(provisions),
			)
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}

	// Aggregate panicErrs into fetchErrors so a backend that panicked
	// is treated the same as a backend that errored: reconciliation
	// aborts rather than proceeding with partial data (which would
	// misidentify real provisions as orphans and deprovision them).
	if len(panicErrs) > 0 {
		fetchErrors = append(fetchErrors, panicErrs...)
	}

	if len(fetchErrors) > 0 {
		return nil, fmt.Errorf("aborting reconciliation due to incomplete backend data: %w", errors.Join(fetchErrors...))
	}

	return allProvisions, nil
}

// fetchAllRetentions queries every backend's retained leases in parallel,
// returning leaseUUID→backendName and whether ALL backends answered
// successfully. The complete flag gates placement pruning (a later task): a
// partial result must never prune (a transient backend outage would look like
// "data gone"). For the additive placement SYNC, a partial result is fine.
func (r *Reconciler) fetchAllRetentions(ctx context.Context) (map[string]string, bool) {
	backends := r.backendRouter.Backends()

	var mu sync.Mutex
	out := make(map[string]string)
	complete := true

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
					complete = false
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
				complete = false
				mu.Unlock()
				return nil // collect from other backends; don't cancel
			}
			mu.Lock()
			for _, ret := range retentions {
				out[ret.LeaseUUID] = b.Name()
			}
			mu.Unlock()
			slog.Debug("fetched backend retentions", "backend", b.Name(), "count", len(retentions))
			return nil
		})
	}
	_ = g.Wait() // closures never return non-nil; errors captured via complete

	return out, complete
}

// handleProvisionError handles errors from provisioning attempts during reconciliation.
// It determines the appropriate action based on error type and lease state:
//   - errLeaseAlreadyInFlight: skip (not a real error)
//   - backend.ErrAlreadyProvisioned: skip (transient race with concurrent Deprovision)
//   - errPayloadNotAvailable: reject (PENDING) or close (ACTIVE) the lease
//   - backend.ErrValidation: reject (PENDING) or close (ACTIVE) the lease
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
	provisioned, acknowledged, anomalies, leaseErrors *atomic.Int32,
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
				if err := r.startProvisioningWithPayload(ctx, lease); err != nil {
					r.handleProvisionError(ctx, err, leaseUUID, lease, &hadError)
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
			if err := r.startProvisioning(ctx, lease); err != nil {
				r.handleProvisionError(ctx, err, leaseUUID, lease, &hadError)
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
		if err := r.startProvisioningWithPayload(ctx, lease); err != nil {
			r.handleProvisionError(ctx, err, leaseUUID, lease, &hadError)
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
		if err := r.startProvisioningWithPayload(ctx, lease); err != nil {
			r.handleProvisionError(ctx, err, leaseUUID, lease, &hadError)
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
			// Lease doesn't exist on chain - orphaned payload
			payloadStore.Delete(leaseUUID)
			cleaned++
			slog.Info("reconcile: cleaned up orphaned payload (lease not found)",
				"lease_uuid", leaseUUID,
			)
			continue
		}

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
//   - retentionsComplete: every backend answered ListRetentions this sweep
//     (provisions completeness is already guaranteed — fetchAllProvisions aborts
//     reconciliation on any backend error before we reach here);
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
	retentionsComplete bool,
	now time.Time,
) int {
	if r.placementStore == nil || !retentionsComplete {
		return 0
	}

	cleaned := 0
	for _, leaseUUID := range r.placementStore.List() {
		if ctx.Err() != nil {
			break
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
		if setAt, ok := r.placementStore.SetAt(leaseUUID); ok && grace > 0 && now.Sub(setAt) < grace {
			// Log the raw timestamps rather than a derived age: now is the
			// sweep-start time, so a placement Set during this sweep has
			// set_at > sweep_start (a negative "age") — the timestamps make
			// that case self-explanatory instead of printing a confusing
			// negative duration.
			slog.Debug("reconcile: keeping placement within grace window",
				"lease_uuid", leaseUUID, "set_at", setAt, "sweep_start", now, "grace", grace)
			continue
		}
		// Chain-terminal, absent from all backends, not in-flight → orphan.
		r.placementStore.Delete(leaseUUID)
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
