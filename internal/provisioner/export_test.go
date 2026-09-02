package provisioner

// export_test.go holds scaffolding that exists ONLY for this package's
// tests. It compiles into the test binary and never into providerd,
// which is the point: provisioner's production files must not carry code
// whose only caller is a test (ENG-354).

import (
	"github.com/ThreeDotsLabs/watermill/message"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backendidentity"
	"github.com/manifest-network/fred/internal/util"
)

var defaultCallbackTestStorageIdentity = func() backendidentity.ID {
	id, err := backendidentity.Parse("6ba7b811-9dad-41d1-80b4-00c04fd430c8")
	if err != nil {
		panic(err)
	}
	return id
}()

type callbackTestStorageIdentityAuthority struct{}

func (callbackTestStorageIdentityAuthority) ExpectedBackendStorageIdentity(
	string,
) (backendidentity.ID, bool) {
	return defaultCallbackTestStorageIdentity, true
}

// newCallbackServiceForTest permits deliberately partial protocol fixtures.
// Production binaries can call only NewCallbackService, whose composition is
// safe by construction. Partial test fixtures receive a concrete, valid
// storage-identity authority; they do not toggle production verification.
func newCallbackServiceForTest(cfg CallbackServiceConfig) (*CallbackService, error) {
	if util.IsNilInterface(cfg.StorageIdentities) {
		cfg.StorageIdentities = callbackTestStorageIdentityAuthority{}
	}
	return newCallbackService(cfg)
}

// HandleBackendCallback preserves the old message-shaped test surface without
// carrying a production method whose only callers are tests. Production HTTP
// ingress already has a decoded DTO and calls HandleBackendCallbackPayload.
func (h *HandlerSet) HandleBackendCallback(msg *message.Message) (err error) {
	defer func() { recordWatermillMetrics(TopicBackendCallback, err) }()

	callback, ok := unmarshalMessagePayload[backend.CallbackPayload](msg, TopicBackendCallback)
	if !ok {
		return nil
	}
	if callback.BackendStorageID == "" {
		callback.BackendStorageID = defaultCallbackTestStorageIdentity.String()
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
	callbacks, err := newCallbackServiceForTest(CallbackServiceConfig{
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
