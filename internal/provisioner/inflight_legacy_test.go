package provisioner

import (
	"time"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/metrics"
)

// These helpers preserve concise setup in older tests without exposing
// generation-free mutation on the production tracker API.

// TrackInFlight installs or replaces an entry for legacy test setup.
func (t *DefaultInFlightTracker) TrackInFlight(
	leaseUUID, tenant string,
	items []backend.LeaseItem,
	backendName string,
) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if p, exists := t.inFlight[leaseUUID]; exists && p.settlementClaimed {
		return
	}
	if _, claimed := t.reconcileClaims[leaseUUID]; claimed {
		return
	}
	t.inFlight[leaseUUID] = InFlightProvision{
		LeaseUUID:  leaseUUID,
		Tenant:     tenant,
		Items:      items,
		Backend:    backendName,
		Generation: t.allocateGenerationLocked(),
		StartTime:  time.Now(),
	}
	t.markMutationLocked(leaseUUID)
	metrics.InFlightProvisions.Set(float64(len(t.inFlight)))
}

// TryTrackInFlight installs a provision entry without requiring a callback
// generation, matching the historical test helper contract.
func (t *DefaultInFlightTracker) TryTrackInFlight(
	leaseUUID, tenant string,
	items []backend.LeaseItem,
	backendName string,
) bool {
	_, ok := t.tryTrack(leaseUUID, tenant, items, backendName, KindProvision, false)
	return ok
}

// TryTrackRestoreInFlight installs the corresponding restore entry for tests.
func (t *DefaultInFlightTracker) TryTrackRestoreInFlight(
	leaseUUID, tenant string,
	items []backend.LeaseItem,
	backendName string,
) bool {
	_, ok := t.tryTrack(leaseUUID, tenant, items, backendName, KindRestore, false)
	return ok
}

// UntrackInFlight removes an unclaimed test entry regardless of generation.
func (t *DefaultInFlightTracker) UntrackInFlight(leaseUUID string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if p, exists := t.inFlight[leaseUUID]; exists && !p.settlementClaimed {
		delete(t.inFlight, leaseUUID)
		t.markMutationLocked(leaseUUID)
		metrics.InFlightProvisions.Set(float64(len(t.inFlight)))
	}
}

// PopInFlight removes and returns an unclaimed test entry.
func (t *DefaultInFlightTracker) PopInFlight(leaseUUID string) (InFlightProvision, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	provision, exists := t.inFlight[leaseUUID]
	if exists && provision.settlementClaimed {
		return InFlightProvision{}, false
	}
	if exists {
		delete(t.inFlight, leaseUUID)
		t.markMutationLocked(leaseUUID)
		metrics.InFlightProvisions.Set(float64(len(t.inFlight)))
	}
	return provision, exists
}

type legacyInFlightTestTracker interface {
	TrackInFlight(string, string, []backend.LeaseItem, string)
	TryTrackInFlight(string, string, []backend.LeaseItem, string) bool
	TryTrackRestoreInFlight(string, string, []backend.LeaseItem, string) bool
	UntrackInFlight(string)
	PopInFlight(string) (InFlightProvision, bool)
}

func (m *Manager) legacyTestTracker() legacyInFlightTestTracker {
	tracker, ok := m.tracker.(legacyInFlightTestTracker)
	if !ok {
		panic("test tracker does not implement legacy setup helpers")
	}
	return tracker
}

// TrackInFlight delegates legacy test setup to the manager's tracker.
func (m *Manager) TrackInFlight(leaseUUID, tenant string, items []backend.LeaseItem, backendName string) {
	m.legacyTestTracker().TrackInFlight(leaseUUID, tenant, items, backendName)
}

// TryTrackInFlight delegates legacy atomic test setup to the tracker.
func (m *Manager) TryTrackInFlight(leaseUUID, tenant string, items []backend.LeaseItem, backendName string) bool {
	return m.legacyTestTracker().TryTrackInFlight(leaseUUID, tenant, items, backendName)
}

// TryTrackRestoreInFlight delegates legacy restore test setup to the tracker.
func (m *Manager) TryTrackRestoreInFlight(leaseUUID, tenant string, items []backend.LeaseItem, backendName string) bool {
	return m.legacyTestTracker().TryTrackRestoreInFlight(leaseUUID, tenant, items, backendName)
}

// UntrackInFlight delegates generation-free test cleanup to the tracker.
func (m *Manager) UntrackInFlight(leaseUUID string) {
	m.legacyTestTracker().UntrackInFlight(leaseUUID)
}

// PopInFlight delegates legacy destructive test inspection to the tracker.
func (m *Manager) PopInFlight(leaseUUID string) (InFlightProvision, bool) {
	return m.legacyTestTracker().PopInFlight(leaseUUID)
}
