package provisioner

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync/atomic"
	"time"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"

	"github.com/manifest-network/fred/internal/backend"
)

// errLeaseAlreadyInFlight indicates the lease is already being provisioned.
// This is not a real error - the caller should not treat it as a failure.
var errLeaseAlreadyInFlight = errors.New("lease already in-flight")

// ReconcilerChainClient defines the chain operations needed by the reconciler.
type ReconcilerChainClient interface {
	GetPendingLeases(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error)
	GetActiveLeasesByProvider(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error)
	AcknowledgeLeases(ctx context.Context, leaseUUIDs []string) (uint64, []string, error)
}

// Reconciler performs level-triggered reconciliation between chain state and backend state.
// It ensures consistency by comparing current state rather than replaying events.
type Reconciler struct {
	providerUUID    string
	callbackBaseURL string
	chainClient     ReconcilerChainClient
	backendRouter   *backend.Router
	manager         *Manager // For tracking in-flight provisions (shared state with event-driven path)

	interval    time.Duration
	reconciling atomic.Bool // Non-blocking flag to prevent concurrent reconciliation
}

// ReconcilerConfig configures the reconciler.
type ReconcilerConfig struct {
	ProviderUUID    string
	CallbackBaseURL string
	Interval        time.Duration // How often to run periodic reconciliation
}

// NewReconciler creates a new reconciler.
func NewReconciler(cfg ReconcilerConfig, chainClient ReconcilerChainClient, backendRouter *backend.Router, manager *Manager) (*Reconciler, error) {
	if chainClient == nil {
		return nil, fmt.Errorf("chainClient is required")
	}
	if backendRouter == nil {
		return nil, fmt.Errorf("backendRouter is required")
	}
	if cfg.ProviderUUID == "" {
		return nil, fmt.Errorf("ProviderUUID is required")
	}
	if cfg.CallbackBaseURL == "" {
		return nil, fmt.Errorf("CallbackBaseURL is required")
	}

	// Default interval
	interval := cfg.Interval
	if interval == 0 {
		interval = 5 * time.Minute
	}

	return &Reconciler{
		providerUUID:    cfg.ProviderUUID,
		callbackBaseURL: cfg.CallbackBaseURL,
		chainClient:     chainClient,
		backendRouter:   backendRouter,
		manager:         manager,
		interval:        interval,
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
func (r *Reconciler) ReconcileAll(ctx context.Context) error {
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

	// 2. Get provisions from ALL backends
	allProvisions := make(map[string]backend.ProvisionInfo)
	for _, b := range r.backendRouter.Backends() {
		provisions, err := b.ListProvisions(ctx)
		if err != nil {
			slog.Error("failed to list provisions from backend",
				"backend", b.Name(),
				"error", err,
			)
			// Continue with other backends - partial reconciliation is better than none
			continue
		}

		for _, p := range provisions {
			// Create an enriched copy with backend name (p is already a copy since
			// ProvisionInfo is a value type; we're not modifying the original slice)
			p.BackendName = b.Name()
			allProvisions[p.LeaseUUID] = p
		}

		slog.Debug("fetched backend provisions",
			"backend", b.Name(),
			"count", len(provisions),
		)
	}

	slog.Info("fetched backend provisions", "total", len(allProvisions))

	// Check for cancellation before reconciliation loop
	if err := ctx.Err(); err != nil {
		return err
	}

	// 3. Reconcile each chain lease
	var (
		provisioned  int
		acknowledged int
		anomalies    int
	)

	for leaseUUID, lease := range chainLeases {
		provision, isProvisioned := allProvisions[leaseUUID]

		switch {
		case lease.State == billingtypes.LEASE_STATE_PENDING && !isProvisioned:
			// TODO(phase-4): Check for meta_hash and await payload upload
			// before starting provisioning. For now, start immediately.
			if err := r.startProvisioning(ctx, lease); err != nil {
				// errLeaseAlreadyInFlight is not a real error - the event-driven
				// path is handling this lease, so we just skip it.
				if errors.Is(err, errLeaseAlreadyInFlight) {
					slog.Debug("reconcile: lease already in-flight, skipping",
						"lease_uuid", leaseUUID,
					)
				} else {
					slog.Error("reconcile: failed to start provisioning",
						"lease_uuid", leaseUUID,
						"error", err,
					)
				}
			} else {
				provisioned++
			}

		case lease.State == billingtypes.LEASE_STATE_PENDING && isProvisioned && provision.Status == backend.ProvisionStatusReady:
			// Provisioned but not acknowledged - acknowledge now
			if err := r.acknowledgeLease(ctx, leaseUUID); err != nil {
				slog.Error("reconcile: failed to acknowledge lease",
					"lease_uuid", leaseUUID,
					"error", err,
				)
			} else {
				acknowledged++
			}

		case lease.State == billingtypes.LEASE_STATE_PENDING && isProvisioned && provision.Status == backend.ProvisionStatusProvisioning:
			// Provisioning in progress - wait for callback
			slog.Debug("reconcile: lease provisioning in progress",
				"lease_uuid", leaseUUID,
			)

		case lease.State == billingtypes.LEASE_STATE_PENDING && isProvisioned && provision.Status == backend.ProvisionStatusFailed:
			// Provisioning failed - lease will eventually expire
			// TODO(phase-3): Consider calling RejectLease here
			slog.Warn("reconcile: lease provisioning failed, waiting for expiry",
				"lease_uuid", leaseUUID,
				"tenant", lease.Tenant,
			)

		case lease.State == billingtypes.LEASE_STATE_ACTIVE && !isProvisioned:
			// Anomaly: Lease is active but not provisioned
			// This shouldn't happen in normal operation
			slog.Warn("reconcile: anomaly - active lease not provisioned",
				"lease_uuid", leaseUUID,
				"tenant", lease.Tenant,
			)
			anomalies++
			// Attempt to provision
			if err := r.startProvisioning(ctx, lease); err != nil {
				if !errors.Is(err, errLeaseAlreadyInFlight) {
					slog.Error("reconcile: failed to provision anomalous lease",
						"lease_uuid", leaseUUID,
						"error", err,
					)
				}
			}

		case lease.State == billingtypes.LEASE_STATE_ACTIVE && isProvisioned:
			// Healthy state - nothing to do
		}

		// Remove from allProvisions to track orphans
		delete(allProvisions, leaseUUID)
	}

	// 4. Remaining provisions have no lease - check for orphans
	// Only deprovision orphans that belong to this provider to avoid
	// interfering with other providers sharing the same backend.
	var orphans int
	for leaseUUID, provision := range allProvisions {
		// Skip provisions that belong to a different provider
		if provision.ProviderUUID != "" && provision.ProviderUUID != r.providerUUID {
			slog.Debug("reconcile: skipping provision owned by different provider",
				"lease_uuid", leaseUUID,
				"provision_provider", provision.ProviderUUID,
				"our_provider", r.providerUUID,
			)
			continue
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
			continue
		}

		orphans++
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
		}
	}

	slog.Info("reconciliation complete",
		"provisioned", provisioned,
		"acknowledged", acknowledged,
		"anomalies", anomalies,
		"orphans", orphans,
	)

	return nil
}

// startProvisioning initiates provisioning for a lease.
// Returns errLeaseAlreadyInFlight if the lease is already being provisioned by
// the event-driven path (this is not a real error, just a signal to skip).
func (r *Reconciler) startProvisioning(ctx context.Context, lease billingtypes.Lease) error {
	// TODO(phase-3): Use SKU-based routing when implemented
	// backendClient := r.backendRouter.Route(lease.Sku)
	backendClient := r.backendRouter.Default()
	if backendClient == nil {
		return fmt.Errorf("no backend available")
	}

	// Atomically track in manager's in-flight map if manager is available.
	// This prevents TOCTOU race between the reconciler and event-driven path:
	// both may try to provision the same lease concurrently.
	if r.manager != nil {
		// TODO(phase-3): Extract SKU from lease.Items for routing
		if !r.manager.TryTrackInFlight(lease.Uuid, lease.Tenant, "", backendClient.Name()) {
			return errLeaseAlreadyInFlight
		}
	}

	err := backendClient.Provision(ctx, backend.ProvisionRequest{
		LeaseUUID:    lease.Uuid,
		Tenant:       lease.Tenant,
		ProviderUUID: r.providerUUID,
		// TODO(phase-3): Extract SKU from lease.Items for routing
		CallbackURL: r.callbackURL(),
	})
	if err != nil {
		// Clean up in-flight on error
		if r.manager != nil {
			r.manager.UntrackInFlight(lease.Uuid)
		}
		return err
	}

	slog.Info("reconcile: started provisioning",
		"lease_uuid", lease.Uuid,
		"tenant", lease.Tenant,
		"backend", backendClient.Name(),
	)

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

// callbackURL returns the callback URL for backend provisioning.
func (r *Reconciler) callbackURL() string {
	return r.callbackBaseURL + CallbackPath
}
