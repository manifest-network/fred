package provisioner

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/metrics"
)

// ProvisionOpts contains optional parameters for provisioning.
type ProvisionOpts struct {
	Payload     []byte // Optional deployment payload
	PayloadHash string // Optional hex-encoded SHA-256 hash of payload
}

// ProvisionOrchestrator coordinates the provisioning flow.
// It routes to the appropriate backend, tracks the provision in-flight,
// and initiates the async provisioning call.
type ProvisionOrchestrator struct {
	providerUUID    string
	callbackBaseURL string
	router          BackendRouter
	tracker         InFlightTracker
	placementStore  PlacementStore
}

// NewProvisionOrchestrator creates a new ProvisionOrchestrator.
func NewProvisionOrchestrator(providerUUID, callbackBaseURL string, router BackendRouter, tracker InFlightTracker, placementStore PlacementStore) *ProvisionOrchestrator {
	return &ProvisionOrchestrator{
		providerUUID:    providerUUID,
		callbackBaseURL: callbackBaseURL,
		router:          router,
		tracker:         tracker,
		placementStore:  placementStore,
	}
}

// StartProvisioning handles the common provisioning flow for both lease creation
// and payload-triggered provisioning. It routes to the appropriate backend,
// tracks the provision in-flight, and initiates the async provisioning call.
//
// Returns nil if provisioning was started successfully or the lease is already in-flight.
// Returns an error if routing fails or the backend call fails.
func (o *ProvisionOrchestrator) StartProvisioning(ctx context.Context, lease *billingtypes.Lease, opts ProvisionOpts) error {
	// Extract lease items and primary SKU for routing
	items := ExtractLeaseItems(lease)
	sku := ExtractRoutingSKU(lease)
	totalQuantity := TotalLeaseQuantity(lease)

	// Route to appropriate backend, honoring existing placement for restored/placed leases (ENG-333)
	backendClient := routeForProvisionHonoringPlacement(ctx, o.router, o.placementStore, lease.Uuid, sku, o.tracker.InFlightCountsByBackend())
	if backendClient == nil {
		slog.Error("no backend available for provisioning",
			"lease_uuid", lease.Uuid,
			"sku", sku,
		)
		return fmt.Errorf("%w: lease %s", ErrNoBackendAvailable, lease.Uuid)
	}

	// Atomically track in-flight BEFORE calling Provision to prevent:
	// 1. Race with reconciler (TOCTOU between IsInFlight check and TrackInFlight)
	// 2. Race with fast backend response (callback arriving before tracking)
	if !o.tracker.TryTrackInFlight(lease.Uuid, lease.Tenant, items, backendClient.Name()) {
		slog.Debug("lease already in-flight, skipping",
			"lease_uuid", lease.Uuid,
		)
		return nil
	}

	// Build provision request
	req := backend.ProvisionRequest{
		LeaseUUID:    lease.Uuid,
		Tenant:       lease.Tenant,
		ProviderUUID: o.providerUUID,
		Items:        items,
		CallbackURL:  BuildCallbackURL(o.callbackBaseURL),
		Payload:      opts.Payload,
	}
	// Only include PayloadHash when we have the actual payload
	if opts.Payload != nil && opts.PayloadHash != "" {
		req.PayloadHash = opts.PayloadHash
	}

	// Start provisioning (async - backend will call back)
	if err := backendClient.Provision(ctx, req); err != nil {
		if errors.Is(err, backend.ErrInsufficientResources) {
			metrics.BackendInsufficientResourcesTotal.WithLabelValues(backendClient.Name()).Inc()
		}
		// Clean up in-flight tracking on failure
		o.tracker.UntrackInFlight(lease.Uuid)

		slog.Error("failed to start provisioning",
			"lease_uuid", lease.Uuid,
			"sku", sku,
			"total_quantity", totalQuantity,
			"backend", backendClient.Name(),
			"error", err,
		)
		return fmt.Errorf("%w: %w", ErrProvisioningFailed, err)
	}

	// Record placement so read operations can find this lease's backend
	if o.placementStore != nil {
		if err := o.placementStore.Set(lease.Uuid, backendClient.Name()); err != nil {
			slog.Warn("failed to record placement",
				"lease_uuid", lease.Uuid,
				"backend", backendClient.Name(),
				"error", err,
			)
		}
	}

	// Log success with appropriate detail level
	if opts.Payload != nil {
		slog.Info("provisioning started with payload",
			"lease_uuid", lease.Uuid,
			"tenant", lease.Tenant,
			"sku", sku,
			"total_quantity", totalQuantity,
			"backend", backendClient.Name(),
			"payload_size", len(opts.Payload),
		)
	} else {
		slog.Info("provisioning started",
			"lease_uuid", lease.Uuid,
			"tenant", lease.Tenant,
			"sku", sku,
			"total_quantity", totalQuantity,
			"backend", backendClient.Name(),
		)
	}

	return nil
}

// routeForProvisionHonoringPlacement returns the backend that already holds the
// lease's data (from placement) when one is recorded and reachable; otherwise it
// falls back to least-loaded selection. This keeps a restored or already-placed
// lease pinned to the backend with its volumes (ENG-333), preventing data drift
// on re-provision/reconcile.
func routeForProvisionHonoringPlacement(
	ctx context.Context,
	router BackendRouter,
	placementStore PlacementStore,
	leaseUUID, sku string,
	inFlightByBackend map[string]int,
) backend.Backend {
	if placementStore != nil {
		if name := placementStore.Get(leaseUUID); name != "" {
			if b := router.GetBackendByName(name); b != nil {
				return b
			}
			slog.Warn("placement backend not found, falling back to least-loaded routing",
				"lease_uuid", leaseUUID,
				"placement_backend", name,
			)
		}
	}
	return router.RouteForProvision(ctx, sku, inFlightByBackend)
}

// DeletePlacement removes the placement record for a lease. Called when a
// lease reaches a terminal state (e.g., rejected after a failure callback)
// without going through the full Deprovision flow.
func (o *ProvisionOrchestrator) DeletePlacement(leaseUUID string) {
	if o.placementStore != nil {
		o.placementStore.Delete(leaseUUID)
	}
}

// RecordRestorePlacement optimistically records the NEW lease's placement after
// a successful restore (the new lease now lives on the backend that held the
// source's retained data). Typed-nil safe. It deliberately does NOT delete the
// source placement: restore is asynchronous (202 + adopt), so deleting source
// state before the adopt confirms is a saga anti-pattern, and source-placement
// cleanup is owned solely by the reconciler (which prunes it once the retention
// disappears from /retentions). This just closes the post-restore reconcile
// window for the new lease (ENG-333).
func (o *ProvisionOrchestrator) RecordRestorePlacement(newLeaseUUID, backendName string) {
	if o.placementStore == nil {
		return
	}
	if err := o.placementStore.Set(newLeaseUUID, backendName); err != nil {
		slog.Warn("failed to record restore placement",
			"lease_uuid", newLeaseUUID, "backend", backendName, "error", err)
	}
}

// Deprovision handles deprovisioning a lease from the appropriate backend.
// It tries to determine the backend in this order:
//  0. From the placement store (most reliable for completed provisions)
//  1. From in-flight tracking (reliable for provisions still awaiting callback)
//  2. Route by SKU using the provided skuHint (consistent with provisioning path)
//  3. Fallback: deprovision from all backends (ensures cleanup even if routing differs)
//
// Returns nil on success or if the lease was not provisioned anywhere.
// Returns an error only if all deprovision attempts fail.
func (o *ProvisionOrchestrator) Deprovision(ctx context.Context, leaseUUID string, skuHint string) error {
	// Try to determine backend from in-flight tracking first
	provision, wasInFlight := o.tracker.PopInFlight(leaseUUID)

	var backendClient backend.Backend

	// Case 0: Check placement store (most reliable for completed provisions)
	if o.placementStore != nil {
		if placedBackend := o.placementStore.Get(leaseUUID); placedBackend != "" {
			backendClient = o.router.GetBackendByName(placedBackend)
			if backendClient != nil {
				slog.Debug("routing deprovision by placement",
					"lease_uuid", leaseUUID,
					"backend", placedBackend,
				)
			} else {
				slog.Warn("placement backend not found, will try other methods",
					"lease_uuid", leaseUUID,
					"backend_name", placedBackend,
				)
			}
		}
	}

	if backendClient == nil && wasInFlight && provision.Backend != "" {
		// Case 1: Was in-flight - use the tracked backend
		backendClient = o.router.GetBackendByName(provision.Backend)
		if backendClient == nil {
			slog.Warn("backend not found by name, will route by SKU",
				"lease_uuid", leaseUUID,
				"backend_name", provision.Backend,
			)
		}
	}

	if backendClient == nil && skuHint != "" {
		// Case 2: Try to route by SKU
		backendClient = o.router.Route(skuHint)
		if backendClient != nil {
			slog.Debug("routing deprovision by SKU",
				"lease_uuid", leaseUUID,
				"sku", skuHint,
				"backend", backendClient.Name(),
			)
		}
	}

	if backendClient != nil {
		// Deprovision from the determined backend
		if err := backendClient.Deprovision(ctx, leaseUUID); err != nil {
			slog.Error("failed to deprovision",
				"lease_uuid", leaseUUID,
				"backend", backendClient.Name(),
				"error", err,
			)
			return fmt.Errorf("%w: lease %s: %w", ErrDeprovisionFailed, leaseUUID, err)
		}

		// Placement is intentionally NOT deleted here (ENG-333). It is a derived
		// index of where the lease's data lives; if the backend retained the
		// volumes, the placement must survive close so a restore can route to it.
		// The reconciler is the sole pruner (cleanupOrphanedPlacements), gated on
		// the lease being terminal on chain AND absent from all backends.

		slog.Info("deprovisioned successfully",
			"lease_uuid", leaseUUID,
			"backend", backendClient.Name(),
		)
		return nil
	}

	// Case 3: Fallback - deprovision from all backends
	// This ensures cleanup even if we can't determine the correct backend
	slog.Warn("could not determine backend for deprovision, trying all backends",
		"lease_uuid", leaseUUID,
	)

	var lastErr error
	deprovisioned := false
	for _, b := range o.router.Backends() {
		if err := b.Deprovision(ctx, leaseUUID); err != nil {
			slog.Debug("deprovision from backend returned error",
				"lease_uuid", leaseUUID,
				"backend", b.Name(),
				"error", err,
			)
			lastErr = err
		} else {
			slog.Info("deprovisioned successfully",
				"lease_uuid", leaseUUID,
				"backend", b.Name(),
			)
			deprovisioned = true
		}
	}

	// Placement is intentionally NOT deleted here (ENG-333); see resolved-backend path comment.

	if !deprovisioned && lastErr != nil {
		return fmt.Errorf("%w: lease %s: %w", ErrDeprovisionFailed, leaseUUID, lastErr)
	}

	return nil
}
