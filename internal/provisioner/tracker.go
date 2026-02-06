package provisioner

import (
	"context"
	"log/slog"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/metrics"
	"github.com/manifest-network/fred/internal/provisioner/payload"
)

// InFlightProvision represents a lease that is currently being provisioned.
type InFlightProvision struct {
	LeaseUUID string
	Tenant    string
	Items     []backend.LeaseItem // All items being provisioned
	Backend   string
	StartTime time.Time // For duration metrics
}

// RoutingSKU returns the first SKU for backend routing decisions.
//
// Used during deprovision when we need to determine which backend handled
// a lease but only have the in-flight tracking data. Since all items in a
// lease belong to the same provider, any SKU works for routing.
//
// This should NOT be used for resource calculations - use Items directly.
func (p InFlightProvision) RoutingSKU() string {
	if len(p.Items) == 0 {
		return ""
	}
	return p.Items[0].SKU
}

// InFlightTracker defines the interface for tracking in-flight provisions.
// This is used by handlers, orchestrator, timeout checker, and reconciler.
type InFlightTracker interface {
	// TryTrackInFlight atomically checks if a lease is already in-flight and tracks it if not.
	// Returns true if the lease was successfully tracked (was not already in-flight),
	// false if the lease was already being provisioned.
	TryTrackInFlight(leaseUUID, tenant string, items []backend.LeaseItem, backendName string) bool

	// TrackInFlight registers a lease as being provisioned.
	TrackInFlight(leaseUUID, tenant string, items []backend.LeaseItem, backendName string)

	// UntrackInFlight removes a lease from the in-flight tracking.
	UntrackInFlight(leaseUUID string)

	// PopInFlight atomically removes and returns an in-flight provision.
	// Returns the provision info and true if found, or zero value and false if not found.
	PopInFlight(leaseUUID string) (InFlightProvision, bool)

	// GetInFlight returns the in-flight provision info without removing it.
	// Returns the provision info and true if found, or zero value and false if not found.
	GetInFlight(leaseUUID string) (InFlightProvision, bool)

	// IsInFlight checks if a lease is currently being provisioned.
	IsInFlight(leaseUUID string) bool

	// InFlightCount returns the number of provisions currently in flight.
	InFlightCount() int

	// GetInFlightLeases returns a snapshot of all in-flight lease UUIDs.
	GetInFlightLeases() []string

	// WaitForDrain waits for all in-flight provisions to complete, up to the given timeout.
	// Returns the number of provisions that were still in-flight when the timeout expired.
	WaitForDrain(ctx context.Context, timeout time.Duration) int

	// GetTimedOutProvisions returns provisions that have exceeded the given timeout.
	GetTimedOutProvisions(timeout time.Duration) []InFlightProvision
}

// ReconcilerTracker extends InFlightTracker with payload-related methods
// needed by the reconciler for coordinating with the event-driven path.
type ReconcilerTracker interface {
	InFlightTracker

	// HasPayload checks if a payload exists for a lease.
	// Returns an error if the underlying store read fails.
	HasPayload(leaseUUID string) (bool, error)

	// PayloadStore returns the payload store for direct access.
	// May return nil if payload store is not configured.
	PayloadStore() *payload.Store
}

// DefaultInFlightTracker is the default implementation of InFlightTracker.
// It uses a sync.RWMutex for thread-safe tracking of in-flight provisions.
type DefaultInFlightTracker struct {
	inFlight map[string]InFlightProvision
	mu       sync.RWMutex
}

// NewInFlightTracker creates a new DefaultInFlightTracker.
func NewInFlightTracker() *DefaultInFlightTracker {
	return &DefaultInFlightTracker{
		inFlight: make(map[string]InFlightProvision),
	}
}

// Compile-time check that DefaultInFlightTracker implements InFlightTracker.
var _ InFlightTracker = (*DefaultInFlightTracker)(nil)

// TrackInFlight registers a lease as being provisioned.
func (t *DefaultInFlightTracker) TrackInFlight(leaseUUID, tenant string, items []backend.LeaseItem, backendName string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.inFlight[leaseUUID] = InFlightProvision{
		LeaseUUID: leaseUUID,
		Tenant:    tenant,
		Items:     items,
		Backend:   backendName,
		StartTime: time.Now(),
	}
	metrics.InFlightProvisions.Inc()
}

// TryTrackInFlight atomically checks if a lease is already in-flight and tracks it if not.
// Returns true if the lease was successfully tracked (was not already in-flight),
// false if the lease was already being provisioned.
func (t *DefaultInFlightTracker) TryTrackInFlight(leaseUUID, tenant string, items []backend.LeaseItem, backendName string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, exists := t.inFlight[leaseUUID]; exists {
		return false
	}
	t.inFlight[leaseUUID] = InFlightProvision{
		LeaseUUID: leaseUUID,
		Tenant:    tenant,
		Items:     items,
		Backend:   backendName,
		StartTime: time.Now(),
	}
	metrics.InFlightProvisions.Inc()
	return true
}

// UntrackInFlight removes a lease from the in-flight tracking.
func (t *DefaultInFlightTracker) UntrackInFlight(leaseUUID string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, exists := t.inFlight[leaseUUID]; exists {
		delete(t.inFlight, leaseUUID)
		metrics.InFlightProvisions.Dec()
	}
}

// IsInFlight checks if a lease is currently being provisioned.
func (t *DefaultInFlightTracker) IsInFlight(leaseUUID string) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	_, exists := t.inFlight[leaseUUID]
	return exists
}

// PopInFlight atomically removes and returns an in-flight provision.
func (t *DefaultInFlightTracker) PopInFlight(leaseUUID string) (InFlightProvision, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	provision, exists := t.inFlight[leaseUUID]
	if exists {
		delete(t.inFlight, leaseUUID)
		metrics.InFlightProvisions.Dec()
	}
	return provision, exists
}

// GetInFlight returns the in-flight provision info without removing it.
func (t *DefaultInFlightTracker) GetInFlight(leaseUUID string) (InFlightProvision, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	provision, exists := t.inFlight[leaseUUID]
	return provision, exists
}

// InFlightCount returns the number of provisions currently in flight.
func (t *DefaultInFlightTracker) InFlightCount() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.inFlight)
}

// GetInFlightLeases returns a snapshot of all in-flight lease UUIDs.
func (t *DefaultInFlightTracker) GetInFlightLeases() []string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return slices.Collect(maps.Keys(t.inFlight))
}

// WaitForDrain waits for all in-flight provisions to complete, up to the given timeout.
// Returns the number of provisions that were still in-flight when the timeout expired.
func (t *DefaultInFlightTracker) WaitForDrain(ctx context.Context, timeout time.Duration) int {
	if t.InFlightCount() == 0 {
		return 0
	}

	slog.Info("waiting for in-flight provisions to drain",
		"count", t.InFlightCount(),
		"timeout", timeout,
	)

	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			remaining := t.InFlightCount()
			if remaining > 0 {
				slog.Warn("drain interrupted by context cancellation",
					"remaining", remaining,
					"leases", t.GetInFlightLeases(),
				)
			}
			return remaining

		case <-ticker.C:
			count := t.InFlightCount()
			if count == 0 {
				slog.Info("all in-flight provisions drained successfully")
				return 0
			}

			if time.Now().After(deadline) {
				slog.Warn("drain timeout expired with provisions still in-flight",
					"remaining", count,
					"leases", t.GetInFlightLeases(),
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

// GetTimedOutProvisions returns provisions that have exceeded the given timeout.
func (t *DefaultInFlightTracker) GetTimedOutProvisions(timeout time.Duration) []InFlightProvision {
	now := time.Now()
	var timedOut []InFlightProvision

	t.mu.RLock()
	defer t.mu.RUnlock()

	for _, p := range t.inFlight {
		if now.Sub(p.StartTime) > timeout {
			timedOut = append(timedOut, p)
		}
	}
	return timedOut
}

// TrackInFlightWithStartTime is a testing helper that allows setting a custom start time.
// This should only be used in tests to simulate old provisions for timeout testing.
func (t *DefaultInFlightTracker) TrackInFlightWithStartTime(leaseUUID, tenant string, items []backend.LeaseItem, backendName string, startTime time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.inFlight[leaseUUID] = InFlightProvision{
		LeaseUUID: leaseUUID,
		Tenant:    tenant,
		Items:     items,
		Backend:   backendName,
		StartTime: startTime,
	}
	metrics.InFlightProvisions.Inc()
}
