package watcher

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/manifest-network/fred/internal/chain"
)

// ChainClient defines the chain client interface needed by the watcher.
type ChainClient interface {
	GetPendingLeases(ctx context.Context, providerUUID string) ([]*LeaseInfo, error)
	AcknowledgeLeases(ctx context.Context, leaseUUIDs []string) (uint64, error)
}

// LeaseInfo represents minimal lease information.
type LeaseInfo struct {
	UUID string
}

// Watcher monitors lease events and auto-acknowledges pending leases.
type Watcher struct {
	client          *chain.Client
	eventSubscriber *chain.EventSubscriber
	providerUUID    string
	autoAcknowledge bool

	pendingLeases map[string]struct{}
	mu            sync.Mutex
}

// New creates a new lease watcher.
func New(client *chain.Client, eventSubscriber *chain.EventSubscriber, providerUUID string, autoAcknowledge bool) *Watcher {
	return &Watcher{
		client:          client,
		eventSubscriber: eventSubscriber,
		providerUUID:    providerUUID,
		autoAcknowledge: autoAcknowledge,
		pendingLeases:   make(map[string]struct{}),
	}
}

// ScanAndAcknowledge scans for pending leases and optionally acknowledges them.
// This should be called once at startup before Start().
// Returns the number of pending leases found.
func (w *Watcher) ScanAndAcknowledge(ctx context.Context) (int, error) {
	slog.Info("scanning for pending leases")

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
		slog.Info("lease acknowledged", "lease_uuid", event.LeaseUUID)

	case chain.LeaseRejected:
		w.removeFromPending(event.LeaseUUID)
		slog.Info("lease rejected", "lease_uuid", event.LeaseUUID)

	case chain.LeaseClosed:
		slog.Info("lease closed", "lease_uuid", event.LeaseUUID)

	case chain.LeaseExpired:
		w.removeFromPending(event.LeaseUUID)
		slog.Info("lease expired", "lease_uuid", event.LeaseUUID)
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

	// Get all pending UUIDs
	var leaseUUIDs []string
	for uuid := range w.pendingLeases {
		leaseUUIDs = append(leaseUUIDs, uuid)
	}
	w.mu.Unlock()

	if err := w.acknowledgeLeases(ctx, leaseUUIDs); err != nil {
		slog.Error("failed to acknowledge leases", "error", err, "count", len(leaseUUIDs))
		return
	}

	// Remove acknowledged leases from pending
	w.mu.Lock()
	for _, uuid := range leaseUUIDs {
		delete(w.pendingLeases, uuid)
	}
	w.mu.Unlock()
}

// acknowledgeLeases acknowledges leases in batches of 100.
func (w *Watcher) acknowledgeLeases(ctx context.Context, leaseUUIDs []string) error {
	const batchSize = 100

	for i := 0; i < len(leaseUUIDs); i += batchSize {
		end := i + batchSize
		if end > len(leaseUUIDs) {
			end = len(leaseUUIDs)
		}
		batch := leaseUUIDs[i:end]

		count, err := w.client.AcknowledgeLeases(ctx, batch)
		if err != nil {
			return err
		}

		slog.Info("acknowledged leases batch", "count", count)
	}

	return nil
}
