package provisioner

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"

	"github.com/manifest-network/fred/internal/backend"
)

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
	manager         *Manager // For publishing events to Watermill

	interval time.Duration
	mu       sync.Mutex
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
// | PENDING     | Provisioned + ready | Acknowledge lease |
// | ACTIVE      | Provisioned | Nothing (healthy) |
// | ACTIVE      | Not provisioned | Anomaly: Log + provision |
// | Not found   | Provisioned | Orphan: Deprovision |
func (r *Reconciler) ReconcileAll(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()

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
			p.BackendName = b.Name()
			allProvisions[p.LeaseUUID] = p
		}

		slog.Debug("fetched backend provisions",
			"backend", b.Name(),
			"count", len(provisions),
		)
	}

	slog.Info("fetched backend provisions", "total", len(allProvisions))

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
				slog.Error("reconcile: failed to start provisioning",
					"lease_uuid", leaseUUID,
					"error", err,
				)
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
				slog.Error("reconcile: failed to provision anomalous lease",
					"lease_uuid", leaseUUID,
					"error", err,
				)
			}

		case lease.State == billingtypes.LEASE_STATE_ACTIVE && isProvisioned:
			// Healthy state - nothing to do
		}

		// Remove from allProvisions to track orphans
		delete(allProvisions, leaseUUID)
	}

	// 4. Remaining provisions have no lease - these are orphans
	orphans := len(allProvisions)
	for leaseUUID, provision := range allProvisions {
		slog.Warn("reconcile: orphan provision found",
			"lease_uuid", leaseUUID,
			"backend", provision.BackendName,
		)

		b := r.backendRouter.GetBackendByName(provision.BackendName)
		if b == nil {
			b = r.backendRouter.Default()
		}
		if b == nil {
			slog.Error("reconcile: no backend available to deprovision orphan",
				"lease_uuid", leaseUUID,
			)
			continue
		}

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
func (r *Reconciler) startProvisioning(ctx context.Context, lease billingtypes.Lease) error {
	// TODO(phase-3): Use SKU-based routing when implemented
	// backendClient := r.backendRouter.Route(lease.Sku)
	backendClient := r.backendRouter.Default()
	if backendClient == nil {
		return fmt.Errorf("no backend available")
	}

	callbackURL := fmt.Sprintf("%s/callbacks/provision", r.callbackBaseURL)

	// Track in manager's in-flight map if manager is available
	// This allows the manager to handle callbacks for this lease
	if r.manager != nil {
		// TODO(phase-3): Extract SKU from lease.Items for routing
		r.manager.TrackInFlight(lease.Uuid, lease.Tenant, "", backendClient.Name())
	}

	err := backendClient.Provision(ctx, backend.ProvisionRequest{
		LeaseUUID:    lease.Uuid,
		Tenant:       lease.Tenant,
		ProviderUUID: r.providerUUID,
		// TODO(phase-3): Extract SKU from lease.Items for routing
		CallbackURL: callbackURL,
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
	slog.Info("starting periodic reconciliation", "interval", r.interval)

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
