package provisioner

import (
	"context"
	"fmt"
	"log/slog"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"

	"github.com/manifest-network/fred/internal/backend"
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
}

// NewProvisionOrchestrator creates a new ProvisionOrchestrator.
func NewProvisionOrchestrator(providerUUID, callbackBaseURL string, router BackendRouter, tracker InFlightTracker) *ProvisionOrchestrator {
	return &ProvisionOrchestrator{
		providerUUID:    providerUUID,
		callbackBaseURL: callbackBaseURL,
		router:          router,
		tracker:         tracker,
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

	// Route to appropriate backend based on SKU (Route already falls back to default)
	backendClient := o.router.Route(sku)
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

// Deprovision handles deprovisioning a lease from the appropriate backend.
// It tries to determine the backend in this order:
// 1. From in-flight tracking (most reliable - we know exactly where it's provisioned)
// 2. Route by SKU using the provided skuHint (consistent with provisioning path)
// 3. Fallback: deprovision from all backends (ensures cleanup even if routing differs)
//
// Returns nil on success or if the lease was not provisioned anywhere.
// Returns an error only if all deprovision attempts fail.
func (o *ProvisionOrchestrator) Deprovision(ctx context.Context, leaseUUID string, skuHint string) error {
	// Try to determine backend from in-flight tracking first
	provision, wasInFlight := o.tracker.PopInFlight(leaseUUID)

	var backendClient backend.Backend

	if wasInFlight && provision.Backend != "" {
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

	if !deprovisioned && lastErr != nil {
		return fmt.Errorf("%w: lease %s: %w", ErrDeprovisionFailed, leaseUUID, lastErr)
	}

	return nil
}
