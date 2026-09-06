package provisioner

// export_test.go holds scaffolding that exists ONLY for this package's tests.
// It compiles into the test binary and never into providerd.

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/ThreeDotsLabs/watermill/message"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/hmacauth"
	"github.com/manifest-network/fred/internal/provisioner/placement"
)

const callbackTestHMACSecret = "callback-test-secret-0123456789abcdef"

var (
	callbackTestProofVerifier, callbackTestProofConsumer = hmacauth.NewCallbackProofBoundary()
	callbackTestCoordinators                             sync.Map
)

// callbackServiceTestConfig mirrors the production construction boundary.
type callbackServiceTestConfig struct {
	Coordinator         *placement.OperationCoordinator
	Chain               CallbackChain
	Acknowledger        Acknowledger
	Payloads            CallbackPayloadStore
	Events              CallbackEventSink
	Backends            CallbackBackendCatalog
	DeprovisionObserver CallbackDeprovisionObserver
}

func newCallbackServiceForTest(cfg callbackServiceTestConfig) (*CallbackService, error) {
	if cfg.Chain == nil {
		cfg.Chain = &callbackChainStub{}
	}
	if cfg.Acknowledger == nil {
		cfg.Acknowledger = callbackAcknowledgerFunc(func(
			context.Context, string,
		) (bool, string, error) {
			return true, "", nil
		})
	}
	var coordinator *placement.AuthenticatedCallbackCoordinator
	var err error
	if cfg.Coordinator != nil {
		coordinator, err = authenticatedCallbackCoordinatorForTest(
			cfg.Coordinator,
			cfg.Chain, cfg.Acknowledger,
		)
		if err != nil {
			return nil, err
		}
	}
	service, err := NewCallbackService(CallbackServiceConfig{
		Coordinator: coordinator,
		Payloads:    cfg.Payloads, Events: cfg.Events, Backends: cfg.Backends,
		DeprovisionObserver: cfg.DeprovisionObserver,
	})
	if err == nil && cfg.Coordinator != nil {
		callbackTestCoordinators.Store(service, cfg.Coordinator)
	}
	return service, err
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
		backendName := "test-backend"
		if service, serviceOK := h.callbacks.(*CallbackService); serviceOK {
			if raw, exists := callbackTestCoordinators.Load(service); exists {
				metadata, tracked := raw.(*placement.OperationCoordinator).Lookup(callback.LeaseUUID)
				if tracked {
					backendName = metadata.Backend()
				}
			} else if callback.Backend != "" {
				backendName = callback.Backend
			}
		}
		callback.BackendStorageID = testBackendStorageID(backendName).String()
	}
	proof, err := callbackProofForTest(callback)
	if err != nil {
		return err
	}
	return h.HandleBackendCallbackEvidence(msg.Context(), proof)
}

func callbackProofForTest(callback backend.CallbackPayload) (hmacauth.VerifiedRequest, error) {
	query := make(url.Values)
	if callback.OperationID != "" {
		query.Set(backend.CallbackOperationIDQueryParameter, callback.OperationID)
	}
	if callback.LifecycleID != "" {
		query.Set("lifecycle_id", callback.LifecycleID)
	}
	callback.OperationID = ""
	callback.LifecycleID = ""
	body, err := json.Marshal(callback)
	if err != nil {
		return hmacauth.VerifiedRequest{}, err
	}
	uri := "/callbacks/provision"
	if encoded := query.Encode(); encoded != "" {
		uri += "?" + encoded
	}
	now := time.Now()
	signature := hmacauth.SignWithTime(callbackTestHMACSecret, http.MethodPost, uri, body, now)
	return callbackTestProofVerifier.VerifyRoutedWithTime(
		callbackTestHMACSecret, http.MethodPost, uri, body, signature,
		callback.BackendStorageID, "/callbacks/provision",
		5*time.Minute, time.Minute, now,
	)
}

// handlersOf builds a HandlerSet from the same dependencies Manager holds.
// Call this once per test and reuse the result because HandlerSet carries
// mutable payload-waiting state.
func handlersOf(m *Manager) *HandlerSet {
	return m.handlers
}
