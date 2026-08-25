package provisioner

// export_test.go holds scaffolding that exists ONLY for this package's
// tests. It compiles into the test binary and never into providerd,
// which is the point: provisioner's production files must not carry code
// whose only caller is a test (ENG-354).

import (
	"time"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/metrics"
)

// handlersOf builds the HandlerSet that NewManager wires into the
// Watermill router, from the same dependencies m already holds.
//
// NewManager keeps that set in a local and registers its methods as
// router handlers; it deliberately keeps no field pointing at it, because
// a field production writes and only tests read is test scaffolding in a
// production struct (ENG-765). Tests that want to invoke a handler
// directly — rather than by publishing a message and waiting on the
// router — rebuild it here, through the production constructor, so this
// stays honest if HandlerDeps grows a field: adding one to HandlerDeps
// without adding it here yields a set the compiler accepts and the tests
// exercise differently from production.
//
// Call this ONCE per test and reuse the result. HandlerSet carries mutable
// state (awaitingPayload, which HandleLeaseCreated fills and the payload
// and callback handlers drain, feeding the leases-awaiting-payload gauge),
// so a fresh set per call would silently drop it between handler
// invocations in a test that spans more than one.
func handlersOf(m *Manager) *HandlerSet {
	return NewHandlerSet(HandlerDeps{
		ChainClient:   m.chainClient,
		Orchestrator:  m.orchestrator,
		Tracker:       m.tracker,
		Acknowledger:  m.ackBatcher,
		PayloadStore:  m.payloadStore,
		Publisher:     m.publisher,
		BackendRouter: m.router,
	})
}

// TrackInFlightWithStartTime records an in-flight provision with a
// caller-supplied start time so timeout tests can simulate a provision
// that began in the past. Production always stamps time.Now() via
// TryTrackInFlightWithGeneration.
func (t *DefaultInFlightTracker) TrackInFlightWithStartTime(leaseUUID, tenant string, items []backend.LeaseItem, backendName string, startTime time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, claimed := t.reconcileClaims[leaseUUID]; claimed {
		return
	}
	generation := t.allocateGenerationLocked()
	t.inFlight[leaseUUID] = InFlightProvision{
		LeaseUUID:  leaseUUID,
		Tenant:     tenant,
		Items:      items,
		Backend:    backendName,
		Generation: generation,
		StartTime:  startTime,
	}
	t.markMutationLocked(leaseUUID)
	metrics.InFlightProvisions.Set(float64(len(t.inFlight)))
}
