package watcher

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/manifest-network/fred/internal/chain"
)

// WithdrawTrigger is a function that can be called to trigger a withdrawal.
type WithdrawTrigger func()

// Watcher monitors lease events and auto-acknowledges pending leases.
type Watcher struct {
	client          *chain.Client
	eventSubscriber *chain.EventSubscriber
	providerUUID    string
	autoAcknowledge bool

	pendingLeases   map[string]struct{}
	activeTenants   map[string]struct{} // Tenants with active leases
	mu              sync.Mutex
	withdrawTrigger WithdrawTrigger // Called when cross-provider credit depletion is detected
}

// New creates a new lease watcher.
func New(client *chain.Client, eventSubscriber *chain.EventSubscriber, providerUUID string, autoAcknowledge bool) *Watcher {
	return &Watcher{
		client:          client,
		eventSubscriber: eventSubscriber,
		providerUUID:    providerUUID,
		autoAcknowledge: autoAcknowledge,
		pendingLeases:   make(map[string]struct{}),
		activeTenants:   make(map[string]struct{}),
	}
}

// SetWithdrawTrigger sets the function to call when cross-provider credit depletion is detected.
func (w *Watcher) SetWithdrawTrigger(trigger WithdrawTrigger) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.withdrawTrigger = trigger
}

// ScanAndAcknowledge scans for pending leases and optionally acknowledges them.
// This should be called once at startup before Start().
// Returns the number of pending leases found.
func (w *Watcher) ScanAndAcknowledge(ctx context.Context) (int, error) {
	slog.Info("scanning for pending leases")

	// First, load active tenants for cross-provider credit monitoring
	if err := w.loadActiveTenants(ctx); err != nil {
		slog.Warn("failed to load active tenants", "error", err)
		// Continue - this is not fatal
	}

	leases, err := w.client.GetPendingLeases(ctx, w.providerUUID)
	if err != nil {
		return 0, err
	}

	count := len(leases)
	if count == 0 {
		slog.Info("no pending leases found")
		return 0, nil
	}

	slog.Info("found pending leases", "count", count)

	// If auto-acknowledge is disabled, just report and return
	if !w.autoAcknowledge {
		slog.Info("auto-acknowledge disabled, skipping acknowledgment", "pending_count", count)
		return count, nil
	}

	// Collect lease UUIDs for acknowledgment
	var leaseUUIDs []string
	for _, lease := range leases {
		leaseUUIDs = append(leaseUUIDs, lease.Uuid)
	}

	slog.Info("auto-acknowledging pending leases", "count", len(leaseUUIDs))

	// Acknowledge in batches of 100
	if err := w.acknowledgeLeases(ctx, leaseUUIDs); err != nil {
		return count, err
	}

	return count, nil
}

// loadActiveTenants populates the activeTenants set from current active leases.
func (w *Watcher) loadActiveTenants(ctx context.Context) error {
	leases, err := w.client.GetActiveLeasesByProvider(ctx, w.providerUUID)
	if err != nil {
		return err
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	for _, lease := range leases {
		w.activeTenants[lease.Tenant] = struct{}{}
	}

	slog.Info("loaded active tenants", "count", len(w.activeTenants))
	return nil
}

// Start begins watching for lease events.
// Note: Call ScanAndAcknowledge() before Start() to handle existing pending leases.
func (w *Watcher) Start(ctx context.Context) error {
	slog.Info("starting lease watcher",
		"provider_uuid", w.providerUUID,
		"auto_acknowledge", w.autoAcknowledge,
	)

	// Start event subscriber in a goroutine
	go func() {
		if err := w.eventSubscriber.Start(ctx); err != nil {
			slog.Error("event subscriber error", "error", err)
		}
	}()

	// Process events
	return w.processEvents(ctx)
}

// processEvents handles incoming lease events.
func (w *Watcher) processEvents(ctx context.Context) error {
	batchTicker := time.NewTicker(5 * time.Second)
	defer batchTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case event := <-w.eventSubscriber.Events():
			w.handleEvent(ctx, event)

		case <-batchTicker.C:
			w.processPendingBatch(ctx)
		}
	}
}

// handleEvent processes a single lease event.
func (w *Watcher) handleEvent(ctx context.Context, event chain.LeaseEvent) {
	slog.Debug("handling event",
		"type", event.Type,
		"lease_uuid", event.LeaseUUID,
	)

	switch event.Type {
	case chain.LeaseCreated:
		if w.autoAcknowledge {
			w.queueForAcknowledgment(event.LeaseUUID)
		}

	case chain.LeaseAcknowledged:
		w.removeFromPending(event.LeaseUUID)
		w.addActiveTenant(event.Tenant)
		slog.Info("lease acknowledged", "lease_uuid", event.LeaseUUID, "tenant", event.Tenant)

	case chain.LeaseRejected:
		w.removeFromPending(event.LeaseUUID)
		slog.Info("lease rejected", "lease_uuid", event.LeaseUUID)

	case chain.LeaseClosed:
		slog.Info("lease closed", "lease_uuid", event.LeaseUUID)

	case chain.LeaseExpired:
		w.removeFromPending(event.LeaseUUID)
		slog.Info("lease expired", "lease_uuid", event.LeaseUUID)

	case chain.LeaseAutoClosed:
		// Another provider's withdrawal caused a lease to auto-close due to credit exhaustion.
		// Check if this tenant has active leases with us - if so, trigger a withdrawal
		// to auto-close our leases too.
		w.handleCrossProviderAutoClose(event.Tenant, event.ProviderUUID)
	}
}

// addActiveTenant adds a tenant to the active tenants set.
func (w *Watcher) addActiveTenant(tenant string) {
	if tenant == "" {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	w.activeTenants[tenant] = struct{}{}
}

// handleCrossProviderAutoClose handles lease_auto_closed events from other providers.
// If the tenant has active leases with us, trigger a withdrawal to auto-close them.
func (w *Watcher) handleCrossProviderAutoClose(tenant, eventProviderUUID string) {
	// Ignore auto-close events from our own provider (we handle those normally)
	if eventProviderUUID == w.providerUUID {
		return
	}

	w.mu.Lock()
	_, hasTenant := w.activeTenants[tenant]
	trigger := w.withdrawTrigger
	if !hasTenant {
		w.mu.Unlock()
		slog.Debug("ignoring auto-close event for tenant without active leases",
			"tenant", tenant,
			"event_provider", eventProviderUUID,
		)
		return
	}
	w.mu.Unlock()

	slog.Info("cross-provider credit depletion detected, triggering withdrawal",
		"tenant", tenant,
		"event_provider", eventProviderUUID,
	)

	// trigger is captured while holding the lock; safe because SetWithdrawTrigger
	// is only called once during startup before the watcher processes events.
	if trigger != nil {
		trigger()
	}
}

// queueForAcknowledgment adds a lease to the pending acknowledgment queue.
func (w *Watcher) queueForAcknowledgment(leaseUUID string) {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.pendingLeases[leaseUUID] = struct{}{}
	slog.Debug("queued lease for acknowledgment", "lease_uuid", leaseUUID)
}

// removeFromPending removes a lease from the pending queue.
func (w *Watcher) removeFromPending(leaseUUID string) {
	w.mu.Lock()
	defer w.mu.Unlock()

	delete(w.pendingLeases, leaseUUID)
}

// processPendingBatch processes the pending lease queue in batches.
func (w *Watcher) processPendingBatch(ctx context.Context) {
	w.mu.Lock()
	if len(w.pendingLeases) == 0 {
		w.mu.Unlock()
		return
	}

	// Copy and clear pending leases atomically to avoid race condition.
	// Any new leases added during acknowledgment will be processed in the next batch.
	leaseUUIDs := make([]string, 0, len(w.pendingLeases))
	for uuid := range w.pendingLeases {
		leaseUUIDs = append(leaseUUIDs, uuid)
	}
	// Clear the map while still holding the lock
	clear(w.pendingLeases)
	w.mu.Unlock()

	if err := w.acknowledgeLeases(ctx, leaseUUIDs); err != nil {
		slog.Error("failed to acknowledge leases", "error", err, "count", len(leaseUUIDs))
		// Re-queue failed leases for retry
		w.mu.Lock()
		for _, uuid := range leaseUUIDs {
			w.pendingLeases[uuid] = struct{}{}
		}
		w.mu.Unlock()
		return
	}
}

// acknowledgeLeases acknowledges leases. Batching is handled by the client.
func (w *Watcher) acknowledgeLeases(ctx context.Context, leaseUUIDs []string) error {
	count, txHashes, err := w.client.AcknowledgeLeases(ctx, leaseUUIDs)
	if err != nil {
		return err
	}

	slog.Info("acknowledged leases", "count", count, "tx_hashes", txHashes)
	return nil
}
