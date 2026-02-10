package provisioner

import (
	"cmp"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
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
	backendRouter   *backend.Router
	tracker         ReconcilerTracker // For tracking in-flight provisions (shared state with event-driven path)
	placementStore  PlacementStore    // Optional placement store for round-robin routing

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
// The tracker parameter is optional - if nil, the reconciler will not coordinate with the event-driven path.
// The placementStore parameter is optional - if nil, round-robin placement tracking is disabled.
func NewReconciler(cfg ReconcilerConfig, chainClient ReconcilerChainClient, backendRouter *backend.Router, tracker ReconcilerTracker, placementStore PlacementStore) (*Reconciler, error) {
	if chainClient == nil {
		return nil, errors.New("chain client is required")
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
// | PENDING     | Provisioned + ready | Acknowledge lease |
// | PENDING     | Provisioned + failed | Log warning (wait for expiry) |
// | ACTIVE      | Provisioned | Nothing (healthy) |
// | ACTIVE      | Not provisioned | Anomaly: Log + provision |
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

	// Sync placements from actual backend state (handles cold start and drift).
	// NOTE: This only adds/updates — it never prunes stale records for leases
	// that completed while fred was down. A naive prune is unsafe because a
	// concurrent StartProvisioning may have just Set a placement that backends
	// haven't reported yet. Stale records are harmless (reads fall back to SKU
	// routing) and grow only by the number of leases that close during downtime.
	if r.placementStore != nil && len(allProvisions) > 0 {
		placements := make(map[string]string, len(allProvisions))
		for leaseUUID, provision := range allProvisions {
			if provision.BackendName != "" {
				placements[leaseUUID] = provision.BackendName
			}
		}
		if len(placements) > 0 {
			if err := r.placementStore.SetBatch(placements); err != nil {
				slog.Warn("failed to sync placements from backend state", "error", err)
			}
		}
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
	}

	// 5. Clean up orphaned payloads (payloads for leases that are no longer pending)
	orphanedPayloads := r.cleanupOrphanedPayloads(ctx, chainLeases)

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

	// Route to appropriate backend using round-robin for load distribution
	backendClient := r.backendRouter.RouteRoundRobin(sku)
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
		// Clean up in-flight on error.
		// Keep payload in store so next reconciliation can retry with it.
		if r.tracker != nil {
			r.tracker.UntrackInFlight(lease.Uuid)
		}
		return err
	}

	// Note: Payload is NOT deleted here. It will be deleted by handleBackendCallback
	// after the backend reports success or failure. This ensures the payload remains
	// available for retry if the backend fails or crashes before sending a callback.

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

// acknowledgeLease acknowledges a lease on chain.
func (r *Reconciler) acknowledgeLease(ctx context.Context, leaseUUID string) error {
	acknowledged, txHashes, err := r.chainClient.AcknowledgeLeases(ctx, []string{leaseUUID})
	if err != nil {
		return err
	}

	slog.Info("reconcile: acknowledged lease",
		"lease_uuid", leaseUUID,
		"acknowledged", acknowledged,
		"tx_hashes", txHashes,
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

// cleanupTerminalLease removes stored payload and placement records for a
// lease that has reached a terminal state (rejected or closed).
func (r *Reconciler) cleanupTerminalLease(leaseUUID string) {
	if r.tracker != nil {
		if ps := r.tracker.PayloadStore(); ps != nil {
			ps.Delete(leaseUUID)
		}
	}
	if r.placementStore != nil {
		r.placementStore.Delete(leaseUUID)
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

	for _, b := range backends {
		g.Go(func() error {
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

	if len(fetchErrors) > 0 {
		return nil, fmt.Errorf("aborting reconciliation due to incomplete backend data: %w", errors.Join(fetchErrors...))
	}

	return allProvisions, nil
}

// handleProvisionError handles errors from provisioning attempts during reconciliation.
// It determines the appropriate action based on error type and lease state:
//   - errLeaseAlreadyInFlight: skip (not a real error)
//   - errPayloadNotAvailable: reject (PENDING) or close (ACTIVE) the lease
//   - ErrValidation: reject (PENDING) or close (ACTIVE) the lease
//   - ErrCircuitOpen: reject (PENDING) or close (ACTIVE) the lease
//   - other errors: log and flag for retry next cycle
func (r *Reconciler) handleProvisionError(ctx context.Context, err error, leaseUUID string, lease billingtypes.Lease, hadError *bool) {
	if errors.Is(err, errLeaseAlreadyInFlight) {
		slog.Debug("reconcile: lease already in-flight, skipping", "lease_uuid", leaseUUID)
		return
	}

	// Determine the termination reason for permanent errors
	var reason string
	switch {
	case errors.Is(err, errPayloadNotAvailable):
		reason = "payload not available for re-provisioning"
	case errors.Is(err, backend.ErrValidation):
		reason = validationErrorToRejectReason(err)
	case errors.Is(err, backend.ErrCircuitOpen):
		reason = "backend unavailable"
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
		// Healthy state - nothing to do
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

	// Clean up placement record
	if r.placementStore != nil {
		r.placementStore.Delete(leaseUUID)
	}
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

// Start begins periodic reconciliation.
func (r *Reconciler) Start(ctx context.Context) error {
	// Add jitter (0-25% of interval) to prevent thundering herd when
	// multiple fred instances start simultaneously.
	jitter := time.Duration(rand.Int64N(int64(r.interval / 4)))
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
