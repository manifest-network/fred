package provisioner

import (
	"context"
	"log/slog"
	"time"

	"github.com/manifest-network/fred/internal/metrics"
)

// inFlightProvision represents a lease that is currently being provisioned.
type inFlightProvision struct {
	LeaseUUID string
	Tenant    string
	SKU       string
	Backend   string
	StartTime time.Time // For duration metrics
}

// TrackInFlight registers a lease as being provisioned.
// This allows the manager to handle callbacks for this lease.
func (m *Manager) TrackInFlight(leaseUUID, tenant, sku, backendName string) {
	m.inFlightMu.Lock()
	defer m.inFlightMu.Unlock()
	m.inFlight[leaseUUID] = inFlightProvision{
		LeaseUUID: leaseUUID,
		Tenant:    tenant,
		SKU:       sku,
		Backend:   backendName,
		StartTime: time.Now(),
	}
	metrics.InFlightProvisions.Inc()
}

// TryTrackInFlight atomically checks if a lease is already in-flight and tracks it if not.
// Returns true if the lease was successfully tracked (was not already in-flight),
// false if the lease was already being provisioned.
// This prevents TOCTOU races between checking and tracking.
func (m *Manager) TryTrackInFlight(leaseUUID, tenant, sku, backendName string) bool {
	m.inFlightMu.Lock()
	defer m.inFlightMu.Unlock()
	if _, exists := m.inFlight[leaseUUID]; exists {
		return false
	}
	m.inFlight[leaseUUID] = inFlightProvision{
		LeaseUUID: leaseUUID,
		Tenant:    tenant,
		SKU:       sku,
		Backend:   backendName,
		StartTime: time.Now(),
	}
	metrics.InFlightProvisions.Inc()
	return true
}

// UntrackInFlight removes a lease from the in-flight tracking.
func (m *Manager) UntrackInFlight(leaseUUID string) {
	m.inFlightMu.Lock()
	defer m.inFlightMu.Unlock()
	if _, exists := m.inFlight[leaseUUID]; exists {
		delete(m.inFlight, leaseUUID)
		metrics.InFlightProvisions.Dec()
	}
}

// IsInFlight checks if a lease is currently being provisioned.
func (m *Manager) IsInFlight(leaseUUID string) bool {
	m.inFlightMu.RLock()
	defer m.inFlightMu.RUnlock()
	_, exists := m.inFlight[leaseUUID]
	return exists
}

// PopInFlight atomically removes and returns an in-flight provision.
// Returns the provision info and true if found, or zero value and false if not found.
func (m *Manager) PopInFlight(leaseUUID string) (inFlightProvision, bool) {
	m.inFlightMu.Lock()
	defer m.inFlightMu.Unlock()
	provision, exists := m.inFlight[leaseUUID]
	if exists {
		delete(m.inFlight, leaseUUID)
		metrics.InFlightProvisions.Dec()
	}
	return provision, exists
}

// GetInFlight returns the in-flight provision info without removing it.
// Returns the provision info and true if found, or zero value and false if not found.
func (m *Manager) GetInFlight(leaseUUID string) (inFlightProvision, bool) {
	m.inFlightMu.RLock()
	defer m.inFlightMu.RUnlock()
	provision, exists := m.inFlight[leaseUUID]
	return provision, exists
}

// InFlightCount returns the number of provisions currently in flight.
func (m *Manager) InFlightCount() int {
	m.inFlightMu.RLock()
	defer m.inFlightMu.RUnlock()
	return len(m.inFlight)
}

// GetInFlightLeases returns a snapshot of all in-flight lease UUIDs.
// Used for logging during shutdown.
func (m *Manager) GetInFlightLeases() []string {
	m.inFlightMu.RLock()
	defer m.inFlightMu.RUnlock()

	leases := make([]string, 0, len(m.inFlight))
	for uuid := range m.inFlight {
		leases = append(leases, uuid)
	}
	return leases
}

// WaitForDrain waits for all in-flight provisions to complete, up to the given timeout.
// Returns the number of provisions that were still in-flight when the timeout expired.
// If all provisions complete before the timeout, returns 0.
func (m *Manager) WaitForDrain(ctx context.Context, timeout time.Duration) int {
	if m.InFlightCount() == 0 {
		return 0
	}

	slog.Info("waiting for in-flight provisions to drain",
		"count", m.InFlightCount(),
		"timeout", timeout,
	)

	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			remaining := m.InFlightCount()
			if remaining > 0 {
				slog.Warn("drain interrupted by context cancellation",
					"remaining", remaining,
					"leases", m.GetInFlightLeases(),
				)
			}
			return remaining

		case <-ticker.C:
			count := m.InFlightCount()
			if count == 0 {
				slog.Info("all in-flight provisions drained successfully")
				return 0
			}

			if time.Now().After(deadline) {
				slog.Warn("drain timeout expired with provisions still in-flight",
					"remaining", count,
					"leases", m.GetInFlightLeases(),
				)
				return count
			}

			slog.Debug("waiting for provisions to drain",
				"remaining", count,
				"time_left", time.Until(deadline).Round(time.Second),
			)
		}
	}
}

// runTimeoutChecker periodically checks for timed-out provisions and rejects them.
func (m *Manager) runTimeoutChecker(ctx context.Context) {
	ticker := time.NewTicker(m.timeoutCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.checkCallbackTimeouts(ctx)
		}
	}
}

// checkCallbackTimeouts checks for provisions that have exceeded the callback timeout
// and rejects them so the tenant's credit is released.
func (m *Manager) checkCallbackTimeouts(ctx context.Context) {
	now := time.Now()
	var timedOut []inFlightProvision

	// Find timed-out provisions under read lock
	m.inFlightMu.RLock()
	for _, p := range m.inFlight {
		if now.Sub(p.StartTime) > m.callbackTimeout {
			timedOut = append(timedOut, p)
		}
	}
	m.inFlightMu.RUnlock()

	if len(timedOut) == 0 {
		return
	}

	slog.Warn("found timed-out provisions",
		"count", len(timedOut),
		"timeout", m.callbackTimeout,
	)

	// Process each timed-out provision
	for _, p := range timedOut {
		// Check context before each operation
		if ctx.Err() != nil {
			return
		}

		// Remove from in-flight first
		m.UntrackInFlight(p.LeaseUUID)
		metrics.CallbackTimeoutsTotal.Inc()
		metrics.ProvisioningTotal.WithLabelValues(metrics.OutcomeError, p.Backend).Inc()

		// Record duration (from start until timeout)
		duration := now.Sub(p.StartTime).Seconds()
		metrics.ProvisioningDuration.WithLabelValues(p.Backend).Observe(duration)

		// Reject the lease on chain so tenant's credit is released
		rejected, txHashes, err := m.chainClient.RejectLeases(ctx, []string{p.LeaseUUID}, "callback timeout")
		if err != nil {
			slog.Error("failed to reject timed-out lease",
				"lease_uuid", p.LeaseUUID,
				"error", err,
			)
			// Continue with next - reconciler will pick this up
			continue
		}

		slog.Warn("rejected timed-out provision",
			"lease_uuid", p.LeaseUUID,
			"tenant", p.Tenant,
			"backend", p.Backend,
			"age", now.Sub(p.StartTime),
			"rejected", rejected,
			"tx_hashes", txHashes,
		)
	}
}
