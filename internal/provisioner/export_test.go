package provisioner

// export_test.go holds scaffolding that exists ONLY for this package's
// tests. It compiles into the test binary and never into providerd,
// which is the point: provisioner's production files must not carry code
// whose only caller is a test (ENG-354).

import (
	"time"

	"github.com/ThreeDotsLabs/watermill/message"

	"github.com/manifest-network/fred/internal/backend"
)

// HandleBackendCallback preserves the old message-shaped test surface without
// carrying a production method whose only callers are tests. Production HTTP
// ingress already has a decoded DTO and calls HandleBackendCallbackPayload.
func (h *HandlerSet) HandleBackendCallback(msg *message.Message) (err error) {
	defer func() { recordWatermillMetrics(TopicBackendCallback, err) }()

	callback, ok := unmarshalMessagePayload[backend.CallbackPayload](msg, TopicBackendCallback)
	if !ok {
		return nil
	}
	return h.handleBackendCallbackPayload(msg.Context(), callback)
}

// handlersOf builds a HandlerSet from the same dependencies Manager holds.
//
// NewManager registers the chain and payload methods with Watermill and keeps
// the callback method as its production synchronous ingress function. Older
// manager tests that invoke an adapter directly rebuild the set here, through
// the production constructors. Production callback ordering and direct event
// delivery are covered through Manager.PublishCallback instead; this helper's
// callback event sink intentionally retains the Watermill-shaped test fixture.
//
// Call this ONCE per test and reuse the result. HandlerSet carries mutable
// state (awaitingPayload, which HandleLeaseCreated fills and the payload
// and callback handlers drain, feeding the leases-awaiting-payload gauge),
// so a fresh set per call would silently drop it between handler
// invocations in a test that spans more than one.
func handlersOf(m *Manager) *HandlerSet {
	callbacks, err := NewCallbackService(CallbackServiceConfig{
		Operations:         m.operations,
		Chain:              m.chainClient,
		Acknowledger:       m.ackBatcher,
		Placement:          m.placementStore,
		LifecycleAuthority: m.placementStore,
		Payloads:           m.payloadStore,
		Events: callbackEventSinkFunc(func(
			leaseUUID string, status backend.ProvisionStatus, failure string,
		) {
			publishLeaseStatusEvent(m.publisher, leaseUUID, status, failure)
		}),
		Backends: m.router,
		DeprovisionObserver: callbackDeprovisionObserverFunc(
			m.orchestrator.forgetDeprovisionCandidate,
		),
	})
	if err != nil {
		panic(err)
	}
	return NewHandlerSet(HandlerDeps{
		ChainClient:     m.chainClient,
		Orchestrator:    m.orchestrator,
		EventOperations: m.operations,
		PayloadStore:    m.payloadStore,
		Publisher:       m.publisher,
		Callbacks:       callbacks,
	})
}

// TrackInFlightWithStartTime records an in-flight provision with a
// caller-supplied start time so timeout tests can simulate a provision
// that began in the past. Production always stamps time.Now() via
// TryTrackInFlightWithOperationID.
func (t *DefaultInFlightTracker) TrackInFlightWithStartTime(leaseUUID, tenant string, items []backend.LeaseItem, backendName string, startTime time.Time) {
	t.replaceForLegacy(leaseUUID, tenant, items, backendName, startTime)
}
