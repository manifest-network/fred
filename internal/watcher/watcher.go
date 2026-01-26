package watcher

import (
	"context"
	"log/slog"
	"sync"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"

	"github.com/manifest-network/fred/internal/chain"
)

// WithdrawTrigger is a function that can be called to trigger a withdrawal.
type WithdrawTrigger func()

// ChainClient defines the chain operations needed by the watcher.
type ChainClient interface {
	GetActiveLeasesByProvider(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error)
}

// Watcher monitors lease events for cross-provider credit depletion detection.
// It tracks active tenants and triggers withdrawals when another provider's
// withdrawal causes a tenant's credit to be depleted.
type Watcher struct {
	client          ChainClient
	eventSubscriber *chain.EventSubscriber
	providerUUID    string

	// tenantLeaseCounts tracks the number of active leases per tenant.
	// When the count reaches zero, the tenant is removed from tracking.
	tenantLeaseCounts map[string]int
	mu                sync.Mutex
	withdrawTrigger   WithdrawTrigger // Called when cross-provider credit depletion is detected
}

// New creates a new lease watcher.
func New(client ChainClient, eventSubscriber *chain.EventSubscriber, providerUUID string) *Watcher {
	return &Watcher{
		client:            client,
		eventSubscriber:   eventSubscriber,
		providerUUID:      providerUUID,
		tenantLeaseCounts: make(map[string]int),
	}
}

// SetWithdrawTrigger sets the function to call when cross-provider credit depletion is detected.
func (w *Watcher) SetWithdrawTrigger(trigger WithdrawTrigger) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.withdrawTrigger = trigger
}

// loadActiveTenants populates the tenant lease counts from current active leases.
func (w *Watcher) loadActiveTenants(ctx context.Context) error {
	leases, err := w.client.GetActiveLeasesByProvider(ctx, w.providerUUID)
	if err != nil {
		return err
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	// Count leases per tenant
	for _, lease := range leases {
		w.tenantLeaseCounts[lease.Tenant]++
	}

	slog.Info("loaded active tenants", "count", len(w.tenantLeaseCounts), "total_leases", len(leases))
	return nil
}

// Start begins watching for lease events.
// Note: The EventSubscriber must be started separately (typically in main.go).
func (w *Watcher) Start(ctx context.Context) error {
	slog.Info("starting lease watcher", "provider_uuid", w.providerUUID)

	// Load active tenants for cross-provider credit monitoring
	if err := w.loadActiveTenants(ctx); err != nil {
		slog.Warn("failed to load active tenants", "error", err)
		// Continue - this is not fatal
	}

	// Subscribe to receive events (fan-out: each subscriber gets all events)
	events := w.eventSubscriber.Subscribe()
	defer w.eventSubscriber.Unsubscribe(events)

	// Process events
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case event, ok := <-events:
			if !ok {
				// Channel closed, subscriber is shutting down
				return nil
			}
			w.handleEvent(event)
		}
	}
}

// handleEvent processes a single lease event.
func (w *Watcher) handleEvent(event chain.LeaseEvent) {
	slog.Debug("handling event",
		"type", event.Type,
		"lease_uuid", event.LeaseUUID,
	)

	switch event.Type {
	case chain.LeaseAcknowledged:
		// Track active tenants for cross-provider credit monitoring
		// Only track leases for our provider
		if event.ProviderUUID == w.providerUUID {
			w.addActiveTenant(event.Tenant)
			slog.Info("lease acknowledged", "lease_uuid", event.LeaseUUID, "tenant", event.Tenant)
		}

	case chain.LeaseClosed, chain.LeaseExpired:
		// Decrement lease count when our leases close or expire
		if event.ProviderUUID == w.providerUUID {
			w.removeActiveTenant(event.Tenant)
		}

	case chain.LeaseAutoClosed:
		// Handle auto-close events
		if event.ProviderUUID == w.providerUUID {
			// Our own lease was auto-closed, decrement count
			w.removeActiveTenant(event.Tenant)
		} else {
			// Another provider's withdrawal caused a lease to auto-close due to credit exhaustion.
			// Check if this tenant has active leases with us - if so, trigger a withdrawal
			// to auto-close our leases too.
			w.handleCrossProviderAutoClose(event.Tenant, event.ProviderUUID)
		}
	}
}

// addActiveTenant increments the lease count for a tenant.
func (w *Watcher) addActiveTenant(tenant string) {
	if tenant == "" {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	w.tenantLeaseCounts[tenant]++
}

// removeActiveTenant decrements the lease count for a tenant.
// When the count reaches zero, the tenant is removed from tracking.
func (w *Watcher) removeActiveTenant(tenant string) {
	if tenant == "" {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if count, exists := w.tenantLeaseCounts[tenant]; exists {
		if count <= 1 {
			delete(w.tenantLeaseCounts, tenant)
		} else {
			w.tenantLeaseCounts[tenant] = count - 1
		}
	}
}

// handleCrossProviderAutoClose handles lease_auto_closed events from other providers.
// If the tenant has active leases with us, trigger a withdrawal to auto-close them.
func (w *Watcher) handleCrossProviderAutoClose(tenant, eventProviderUUID string) {
	w.mu.Lock()
	leaseCount := w.tenantLeaseCounts[tenant]
	trigger := w.withdrawTrigger
	w.mu.Unlock()

	if leaseCount == 0 {
		slog.Debug("ignoring auto-close event for tenant without active leases",
			"tenant", tenant,
			"event_provider", eventProviderUUID,
		)
		return
	}

	slog.Info("cross-provider credit depletion detected, triggering withdrawal",
		"tenant", tenant,
		"event_provider", eventProviderUUID,
		"our_lease_count", leaseCount,
	)

	if trigger != nil {
		trigger()
	}
}
