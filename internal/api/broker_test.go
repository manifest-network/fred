package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/testutil"
)

// awaitSubscriber polls until the broker has at least one subscriber for the
// given lease UUID. This replaces fixed time.Sleep calls that were flaky on
// slow CI runners.
func awaitSubscriber(t *testing.T, broker *EventBroker, leaseUUID string) {
	t.Helper()
	require.Eventually(t, func() bool {
		return broker.subscriberCount(leaseUUID) > 0
	}, 2*time.Second, 5*time.Millisecond, "timed out waiting for subscriber on %s", leaseUUID)
}

func testEvent(leaseUUID string, status backend.ProvisionStatus) backend.LeaseStatusEvent {
	return backend.LeaseStatusEvent{
		LeaseUUID: leaseUUID,
		Status:    status,
		Timestamp: time.Now(),
	}
}

// --- EventBroker unit tests ---

func TestEventBroker_SubscribeAndPublish(t *testing.T) {
	broker := NewEventBroker()
	ch, err := broker.Subscribe("lease-1")
	require.NoError(t, err)

	event := testEvent("lease-1", backend.ProvisionStatusReady)
	broker.Publish(event)

	select {
	case received := <-ch:
		assert.Equal(t, "lease-1", received.LeaseUUID)
		assert.Equal(t, backend.ProvisionStatusReady, received.Status)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for event")
	}
}

func TestEventBroker_MultipleClientsForSameLease(t *testing.T) {
	broker := NewEventBroker()
	ch1, err := broker.Subscribe("lease-1")
	require.NoError(t, err)
	ch2, err := broker.Subscribe("lease-1")
	require.NoError(t, err)

	event := backend.LeaseStatusEvent{
		LeaseUUID: "lease-1",
		Status:    backend.ProvisionStatusFailed,
		Error:     "container exited",
		Timestamp: time.Now(),
	}
	broker.Publish(event)

	for _, ch := range []<-chan backend.LeaseStatusEvent{ch1, ch2} {
		select {
		case received := <-ch:
			assert.Equal(t, "lease-1", received.LeaseUUID)
			assert.Equal(t, backend.ProvisionStatusFailed, received.Status)
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for event on one of the clients")
		}
	}
}

func TestEventBroker_UnsubscribeStopsDelivery(t *testing.T) {
	broker := NewEventBroker()
	ch, err := broker.Subscribe("lease-1")
	require.NoError(t, err)

	broker.Unsubscribe("lease-1", ch)

	// Publish after unsubscribe — should not be received (channel is closed).
	broker.Publish(testEvent("lease-1", backend.ProvisionStatusReady))

	// Channel should be closed.
	_, ok := <-ch
	assert.False(t, ok, "expected channel to be closed after unsubscribe")
}

func TestEventBroker_SlowClientDropsEvents(t *testing.T) {
	broker := NewEventBroker()
	ch, err := broker.Subscribe("lease-1")
	require.NoError(t, err)

	// Fill the buffer (eventChannelBuffer = 16).
	for range eventChannelBuffer + 5 {
		broker.Publish(testEvent("lease-1", backend.ProvisionStatusReady))
	}

	// Should not panic or block. We should get exactly eventChannelBuffer events.
	count := 0
	for {
		select {
		case <-ch:
			count++
		default:
			goto done
		}
	}
done:
	assert.Equal(t, eventChannelBuffer, count, "expected exactly buffer-size events, extras should be dropped")
}

func TestEventBroker_DifferentLeasesAreIsolated(t *testing.T) {
	broker := NewEventBroker()
	ch1, err := broker.Subscribe("lease-1")
	require.NoError(t, err)
	ch2, err := broker.Subscribe("lease-2")
	require.NoError(t, err)

	broker.Publish(testEvent("lease-1", backend.ProvisionStatusReady))

	// ch1 should get the event.
	select {
	case received := <-ch1:
		assert.Equal(t, "lease-1", received.LeaseUUID)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for event on ch1")
	}

	// ch2 should NOT get the event.
	select {
	case <-ch2:
		t.Fatal("ch2 should not receive events for lease-1")
	case <-time.After(50 * time.Millisecond):
		// Expected: no event.
	}
}

func TestEventBroker_UnsubscribeNonexistentLease(t *testing.T) {
	broker := NewEventBroker()
	ch := make(chan backend.LeaseStatusEvent)

	// Should not panic.
	require.NotPanics(t, func() {
		broker.Unsubscribe("nonexistent", ch)
	})
}

func TestEventBroker_PublishToNoSubscribers(t *testing.T) {
	broker := NewEventBroker()

	// Should not panic.
	require.NotPanics(t, func() {
		broker.Publish(testEvent("lease-1", backend.ProvisionStatusReady))
	})
}

func TestEventBroker_Close(t *testing.T) {
	broker := NewEventBroker()
	ch1, err := broker.Subscribe("lease-1")
	require.NoError(t, err)
	ch2, err := broker.Subscribe("lease-2")
	require.NoError(t, err)

	broker.Close()

	// All channels should be closed.
	_, ok1 := <-ch1
	assert.False(t, ok1, "expected ch1 to be closed after broker.Close()")
	_, ok2 := <-ch2
	assert.False(t, ok2, "expected ch2 to be closed after broker.Close()")

	// Subscribe after close returns nil.
	ch3, err := broker.Subscribe("lease-3")
	require.NoError(t, err)
	assert.Nil(t, ch3, "expected nil channel from Subscribe after Close")

	// Publish after close should not panic.
	require.NotPanics(t, func() {
		broker.Publish(testEvent("lease-1", backend.ProvisionStatusReady))
	})

	// Double close should not panic.
	require.NotPanics(t, func() {
		broker.Close()
	})
}

func TestEventBroker_PerLeaseSubscriptionLimit(t *testing.T) {
	broker := NewEventBrokerWithLimits(3, 100)

	for range 3 {
		ch, err := broker.Subscribe("lease-1")
		require.NoError(t, err)
		require.NotNil(t, ch)
	}

	// 4th subscription for the same lease should be rejected.
	ch, err := broker.Subscribe("lease-1")
	assert.Nil(t, ch)
	assert.ErrorIs(t, err, ErrTooManySubscriptions)

	// Different lease should still work.
	ch, err = broker.Subscribe("lease-2")
	require.NoError(t, err)
	require.NotNil(t, ch)
}

func TestEventBroker_GlobalSubscriptionLimit(t *testing.T) {
	broker := NewEventBrokerWithLimits(10, 5)

	for i := range 5 {
		ch, err := broker.Subscribe("lease-" + strconv.Itoa(i))
		require.NoError(t, err)
		require.NotNil(t, ch)
	}

	// 6th subscription globally should be rejected.
	ch, err := broker.Subscribe("lease-new")
	assert.Nil(t, ch)
	assert.ErrorIs(t, err, ErrTooManySubscriptions)
}

func TestEventBroker_UnsubscribeFreesSlot(t *testing.T) {
	broker := NewEventBrokerWithLimits(2, 100)

	ch1, err := broker.Subscribe("lease-1")
	require.NoError(t, err)
	ch2, err := broker.Subscribe("lease-1")
	require.NoError(t, err)

	// At limit — next subscribe should fail.
	_, err = broker.Subscribe("lease-1")
	require.ErrorIs(t, err, ErrTooManySubscriptions)

	// Unsubscribe one — should free a slot.
	broker.Unsubscribe("lease-1", ch1)

	ch3, err := broker.Subscribe("lease-1")
	require.NoError(t, err)
	require.NotNil(t, ch3)

	_ = ch2 // keep reference to avoid GC
}

func TestEventBroker_GlobalLimitFreedByUnsubscribe(t *testing.T) {
	broker := NewEventBrokerWithLimits(10, 2)

	ch1, err := broker.Subscribe("lease-1")
	require.NoError(t, err)
	_, err = broker.Subscribe("lease-2")
	require.NoError(t, err)

	// At global limit.
	_, err = broker.Subscribe("lease-3")
	require.ErrorIs(t, err, ErrTooManySubscriptions)

	// Unsubscribe frees global slot.
	broker.Unsubscribe("lease-1", ch1)

	ch3, err := broker.Subscribe("lease-3")
	require.NoError(t, err)
	require.NotNil(t, ch3)
}

// --- WebSocket handler integration tests ---

// wsDialWithAuth dials the WebSocket endpoint with an auth header.
func wsDialWithAuth(t *testing.T, serverURL, leaseUUID, token string) (*websocket.Conn, *http.Response) {
	t.Helper()
	wsURL := "ws" + strings.TrimPrefix(serverURL, "http") + "/v1/leases/" + leaseUUID + "/events"
	header := http.Header{}
	if token != "" {
		header.Set("Authorization", "Bearer "+token)
	}
	conn, resp, err := websocket.DefaultDialer.Dial(wsURL, header)
	if err != nil && conn == nil {
		// Connection failed before upgrade — resp contains the HTTP error.
		return nil, resp
	}
	require.NoError(t, err)
	return conn, resp
}

func newTestHandlers(broker *EventBroker, chainClient ChainClient, providerUUID string) *Handlers {
	return &Handlers{
		client:            chainClient,
		eventBroker:       broker,
		wsUpgrader:        websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }},
		wsMaxMessageSize:  wsDefaultMaxMessageSize,
		wsMaxConnLifetime: wsDefaultMaxConnLifetime,
		providerUUID:      providerUUID,
		bech32Prefix:      "manifest",
	}
}

func TestStreamLeaseEvents_RequiresAuth(t *testing.T) {
	broker := NewEventBroker()
	h := newTestHandlers(broker, nil, testutil.ValidUUID1)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/leases/{lease_uuid}/events", h.StreamLeaseEvents)
	server := httptest.NewServer(mux)
	defer server.Close()

	// Dial without auth header — expect 401.
	_, resp := wsDialWithAuth(t, server.URL, testutil.ValidUUID1, "")
	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
}

func TestStreamLeaseEvents_QueryParamAuth(t *testing.T) {
	broker := NewEventBroker()
	kp := testutil.NewTestKeyPair("test-tenant")
	leaseUUID := testutil.ValidUUID1
	providerUUID := testutil.ValidUUID2
	validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())

	chainClient := &mockChainClient{
		getLeaseFunc: func(ctx context.Context, uuid string) (*billingtypes.Lease, error) {
			return &billingtypes.Lease{
				Uuid:         leaseUUID,
				Tenant:       kp.Address,
				ProviderUuid: providerUUID,
				State:        billingtypes.LEASE_STATE_ACTIVE,
			}, nil
		},
	}

	h := newTestHandlers(broker, chainClient, providerUUID)

	mux := http.NewServeMux()
	mux.Handle("GET /v1/leases/{lease_uuid}/events", WSTokenPromoter(http.HandlerFunc(h.StreamLeaseEvents)))
	server := httptest.NewServer(mux)
	defer server.Close()

	// Dial WITHOUT Authorization header, pass token as query parameter instead.
	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") +
		"/v1/leases/" + leaseUUID + "/events?token=" + validToken
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	require.NoError(t, err)
	defer conn.Close()

	// Verify the connection works by publishing and receiving an event.
	awaitSubscriber(t, broker, leaseUUID)
	broker.Publish(backend.LeaseStatusEvent{
		LeaseUUID: leaseUUID,
		Status:    backend.ProvisionStatusReady,
		Timestamp: time.Now(),
	})

	var event backend.LeaseStatusEvent
	err = conn.ReadJSON(&event)
	require.NoError(t, err)
	assert.Equal(t, leaseUUID, event.LeaseUUID)
	assert.Equal(t, backend.ProvisionStatusReady, event.Status)
}

func TestStreamLeaseEvents_Returns501WhenBrokerNil(t *testing.T) {
	h := &Handlers{
		eventBroker:  nil,
		providerUUID: testutil.ValidUUID1,
		bech32Prefix: "manifest",
	}

	req := httptest.NewRequest("GET", "/v1/leases/"+testutil.ValidUUID1+"/events", nil)
	req.SetPathValue("lease_uuid", testutil.ValidUUID1)

	rec := httptest.NewRecorder()
	h.StreamLeaseEvents(rec, req)

	assert.Equal(t, http.StatusNotImplemented, rec.Code)
}

func TestStreamLeaseEvents_ReceivesEvent(t *testing.T) {
	broker := NewEventBroker()
	kp := testutil.NewTestKeyPair("test-tenant")
	leaseUUID := testutil.ValidUUID1
	providerUUID := testutil.ValidUUID2
	validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())

	chainClient := &mockChainClient{
		getLeaseFunc: func(ctx context.Context, uuid string) (*billingtypes.Lease, error) {
			return &billingtypes.Lease{
				Uuid:         leaseUUID,
				Tenant:       kp.Address,
				ProviderUuid: providerUUID,
				State:        billingtypes.LEASE_STATE_ACTIVE,
			}, nil
		},
	}

	h := newTestHandlers(broker, chainClient, providerUUID)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/leases/{lease_uuid}/events", h.StreamLeaseEvents)
	server := httptest.NewServer(mux)
	defer server.Close()

	conn, _ := wsDialWithAuth(t, server.URL, leaseUUID, validToken)
	require.NotNil(t, conn)
	defer conn.Close()

	// Wait for the server goroutine to subscribe before publishing.
	awaitSubscriber(t, broker, leaseUUID)
	broker.Publish(backend.LeaseStatusEvent{
		LeaseUUID: leaseUUID,
		Status:    backend.ProvisionStatusReady,
		Timestamp: time.Now(),
	})

	var event backend.LeaseStatusEvent
	err := conn.ReadJSON(&event)
	require.NoError(t, err)
	assert.Equal(t, leaseUUID, event.LeaseUUID)
	assert.Equal(t, backend.ProvisionStatusReady, event.Status)
}

func TestStreamLeaseEvents_SurvivesBeyondRequestTimeout(t *testing.T) {
	broker := NewEventBroker()
	kp := testutil.NewTestKeyPair("test-tenant")
	leaseUUID := testutil.ValidUUID1
	providerUUID := testutil.ValidUUID2
	validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())

	chainClient := &mockChainClient{
		getLeaseFunc: func(ctx context.Context, uuid string) (*billingtypes.Lease, error) {
			return &billingtypes.Lease{
				Uuid:         leaseUUID,
				Tenant:       kp.Address,
				ProviderUuid: providerUUID,
				State:        billingtypes.LEASE_STATE_ACTIVE,
			}, nil
		},
	}

	h := newTestHandlers(broker, chainClient, providerUUID)

	// Register WITHOUT timeout middleware — matching production config.
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/leases/{lease_uuid}/events", h.StreamLeaseEvents)
	server := httptest.NewServer(mux)
	defer server.Close()

	conn, _ := wsDialWithAuth(t, server.URL, leaseUUID, validToken)
	require.NotNil(t, conn)
	defer conn.Close()

	// Wait for subscription, then delay 200ms — well beyond a hypothetical
	// 100ms request timeout — to prove the WebSocket connection stays alive
	// without requestTimeoutMiddleware.
	awaitSubscriber(t, broker, leaseUUID)
	time.Sleep(200 * time.Millisecond)
	broker.Publish(backend.LeaseStatusEvent{
		LeaseUUID: leaseUUID,
		Status:    backend.ProvisionStatusReady,
		Timestamp: time.Now(),
	})

	var event backend.LeaseStatusEvent
	err := conn.ReadJSON(&event)
	require.NoError(t, err, "expected event to arrive after delay")
	assert.Equal(t, leaseUUID, event.LeaseUUID)
	assert.Equal(t, backend.ProvisionStatusReady, event.Status)
}

func TestStreamLeaseEvents_WorksThroughLoggingMiddleware(t *testing.T) {
	// Regression test: loggingMiddleware wraps ResponseWriter with responseWriter,
	// which must implement http.Hijacker for WebSocket upgrade to work.
	broker := NewEventBroker()
	kp := testutil.NewTestKeyPair("test-tenant")
	leaseUUID := testutil.ValidUUID1
	providerUUID := testutil.ValidUUID2
	validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())

	chainClient := &mockChainClient{
		getLeaseFunc: func(ctx context.Context, uuid string) (*billingtypes.Lease, error) {
			return &billingtypes.Lease{
				Uuid:         leaseUUID,
				Tenant:       kp.Address,
				ProviderUuid: providerUUID,
				State:        billingtypes.LEASE_STATE_ACTIVE,
			}, nil
		},
	}

	h := newTestHandlers(broker, chainClient, providerUUID)

	// Wrap handler with loggingMiddleware — the same chain as production.
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/leases/{lease_uuid}/events", h.StreamLeaseEvents)
	server := httptest.NewServer(loggingMiddleware(mux))
	defer server.Close()

	conn, _ := wsDialWithAuth(t, server.URL, leaseUUID, validToken)
	require.NotNil(t, conn, "WebSocket should upgrade through loggingMiddleware (responseWriter must implement http.Hijacker)")
	defer conn.Close()

	// Verify an event can actually be received (not just the upgrade).
	awaitSubscriber(t, broker, leaseUUID)
	broker.Publish(backend.LeaseStatusEvent{
		LeaseUUID: leaseUUID,
		Status:    backend.ProvisionStatusReady,
		Timestamp: time.Now(),
	})

	var event backend.LeaseStatusEvent
	err := conn.ReadJSON(&event)
	require.NoError(t, err, "expected event through loggingMiddleware")
	assert.Equal(t, leaseUUID, event.LeaseUUID)
	assert.Equal(t, backend.ProvisionStatusReady, event.Status)
}

func TestStreamLeaseEvents_TimeoutMiddlewareBreaksWebSocket(t *testing.T) {
	// This test documents WHY the WebSocket endpoint must not use requestTimeoutMiddleware.
	// http.TimeoutHandler wraps the ResponseWriter with a buffered writer that does
	// not implement http.Hijacker, so the WebSocket upgrade fails.
	broker := NewEventBroker()
	kp := testutil.NewTestKeyPair("test-tenant")
	leaseUUID := testutil.ValidUUID1
	providerUUID := testutil.ValidUUID2
	validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())

	chainClient := &mockChainClient{
		getLeaseFunc: func(ctx context.Context, uuid string) (*billingtypes.Lease, error) {
			return &billingtypes.Lease{
				Uuid:         leaseUUID,
				Tenant:       kp.Address,
				ProviderUuid: providerUUID,
				State:        billingtypes.LEASE_STATE_ACTIVE,
			}, nil
		},
	}

	h := newTestHandlers(broker, chainClient, providerUUID)

	// Wrap handler WITH timeout middleware — this should break the WebSocket upgrade.
	withTimeout := requestTimeoutMiddleware(5 * time.Second)
	mux := http.NewServeMux()
	mux.Handle("GET /v1/leases/{lease_uuid}/events", withTimeout(http.HandlerFunc(h.StreamLeaseEvents)))
	server := httptest.NewServer(mux)
	defer server.Close()

	// The WebSocket upgrade should fail because http.TimeoutHandler's writer
	// does not implement http.Hijacker.
	conn, resp := wsDialWithAuth(t, server.URL, leaseUUID, validToken)
	if conn != nil {
		conn.Close()
		t.Fatal("expected WebSocket upgrade to fail when wrapped with requestTimeoutMiddleware")
	}
	assert.NotNil(t, resp, "expected HTTP response on failed upgrade")
}

func TestStreamLeaseEvents_CleanCloseOnBrokerShutdown(t *testing.T) {
	broker := NewEventBroker()
	kp := testutil.NewTestKeyPair("test-tenant")
	leaseUUID := testutil.ValidUUID1
	providerUUID := testutil.ValidUUID2
	validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())

	chainClient := &mockChainClient{
		getLeaseFunc: func(ctx context.Context, uuid string) (*billingtypes.Lease, error) {
			return &billingtypes.Lease{
				Uuid:         leaseUUID,
				Tenant:       kp.Address,
				ProviderUuid: providerUUID,
				State:        billingtypes.LEASE_STATE_ACTIVE,
			}, nil
		},
	}

	h := newTestHandlers(broker, chainClient, providerUUID)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/leases/{lease_uuid}/events", h.StreamLeaseEvents)
	server := httptest.NewServer(mux)
	defer server.Close()

	conn, _ := wsDialWithAuth(t, server.URL, leaseUUID, validToken)
	require.NotNil(t, conn)
	defer conn.Close()

	// Wait for the server goroutine to subscribe, then close the broker.
	awaitSubscriber(t, broker, leaseUUID)

	// Close the broker — server should send a CloseGoingAway frame.
	broker.Close()

	// Expect a CloseGoingAway frame.
	_, _, err := conn.ReadMessage()
	require.Error(t, err)
	var closeErr *websocket.CloseError
	require.ErrorAs(t, err, &closeErr)
	assert.Equal(t, websocket.CloseGoingAway, closeErr.Code)
}

func TestStreamLeaseEvents_ClientDisconnect(t *testing.T) {
	broker := NewEventBroker()
	kp := testutil.NewTestKeyPair("test-tenant")
	leaseUUID := testutil.ValidUUID1
	providerUUID := testutil.ValidUUID2
	validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())

	chainClient := &mockChainClient{
		getLeaseFunc: func(ctx context.Context, uuid string) (*billingtypes.Lease, error) {
			return &billingtypes.Lease{
				Uuid:         leaseUUID,
				Tenant:       kp.Address,
				ProviderUuid: providerUUID,
				State:        billingtypes.LEASE_STATE_ACTIVE,
			}, nil
		},
	}

	h := newTestHandlers(broker, chainClient, providerUUID)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/leases/{lease_uuid}/events", h.StreamLeaseEvents)
	server := httptest.NewServer(mux)
	defer server.Close()

	conn, _ := wsDialWithAuth(t, server.URL, leaseUUID, validToken)
	require.NotNil(t, conn)

	// Wait for the server goroutine to subscribe.
	awaitSubscriber(t, broker, leaseUUID)

	// Client sends close frame.
	_ = conn.WriteMessage(websocket.CloseMessage,
		websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
	conn.Close()

	// Wait for the server to process the close and unsubscribe.
	require.Eventually(t, func() bool {
		return broker.subscriberCount(leaseUUID) == 0
	}, 2*time.Second, 5*time.Millisecond, "timed out waiting for unsubscribe")

	// Verify server unsubscribed: publish should not block or panic.
	require.NotPanics(t, func() {
		broker.Publish(testEvent(leaseUUID, backend.ProvisionStatusReady))
	})
}

func TestStreamLeaseEvents_ReceivesRestartAndUpdateEvents(t *testing.T) {
	broker := NewEventBroker()
	kp := testutil.NewTestKeyPair("test-tenant")
	leaseUUID := testutil.ValidUUID1
	providerUUID := testutil.ValidUUID2
	validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())

	chainClient := &mockChainClient{
		getLeaseFunc: func(ctx context.Context, uuid string) (*billingtypes.Lease, error) {
			return &billingtypes.Lease{
				Uuid:         leaseUUID,
				Tenant:       kp.Address,
				ProviderUuid: providerUUID,
				State:        billingtypes.LEASE_STATE_ACTIVE,
			}, nil
		},
	}

	h := newTestHandlers(broker, chainClient, providerUUID)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/leases/{lease_uuid}/events", h.StreamLeaseEvents)
	server := httptest.NewServer(mux)
	defer server.Close()

	conn, _ := wsDialWithAuth(t, server.URL, leaseUUID, validToken)
	require.NotNil(t, conn)
	defer conn.Close()

	// Publish a sequence: restarting → ready, updating → ready.
	statuses := []backend.ProvisionStatus{
		backend.ProvisionStatusRestarting,
		backend.ProvisionStatusReady,
		backend.ProvisionStatusUpdating,
		backend.ProvisionStatusReady,
	}

	awaitSubscriber(t, broker, leaseUUID)
	for _, s := range statuses {
		broker.Publish(backend.LeaseStatusEvent{
			LeaseUUID: leaseUUID,
			Status:    s,
			Timestamp: time.Now(),
		})
	}

	for _, expected := range statuses {
		var event backend.LeaseStatusEvent
		err := conn.ReadJSON(&event)
		require.NoError(t, err)
		assert.Equal(t, leaseUUID, event.LeaseUUID)
		assert.Equal(t, expected, event.Status, "expected status %s", expected)
	}
}

// streamEventsTestEnv builds a Handlers + httptest server + valid token for a
// single lease. Used by the four StreamLeaseEvents read-limit and lifetime
// tests below.
func streamEventsTestEnv(t *testing.T) (h *Handlers, broker *EventBroker, server *httptest.Server, leaseUUID, validToken string) {
	t.Helper()
	broker = NewEventBroker()
	kp := testutil.NewTestKeyPair("test-tenant")
	leaseUUID = testutil.ValidUUID1
	providerUUID := testutil.ValidUUID2
	validToken = testutil.CreateTestToken(kp, leaseUUID, time.Now())

	chainClient := &mockChainClient{
		getLeaseFunc: func(ctx context.Context, uuid string) (*billingtypes.Lease, error) {
			return &billingtypes.Lease{
				Uuid:         leaseUUID,
				Tenant:       kp.Address,
				ProviderUuid: providerUUID,
				State:        billingtypes.LEASE_STATE_ACTIVE,
			}, nil
		},
	}

	h = newTestHandlers(broker, chainClient, providerUUID)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/leases/{lease_uuid}/events", h.StreamLeaseEvents)
	server = httptest.NewServer(mux)
	t.Cleanup(server.Close)
	return h, broker, server, leaseUUID, validToken
}

func TestStreamLeaseEvents_RejectsOversizedFrame(t *testing.T) {
	// The lease events stream is server-push only — clients should never send
	// data frames. The server enforces a small read limit so a buggy or
	// malicious client cannot exhaust memory by sending oversized frames or
	// hold a per-lease subscription slot indefinitely.
	_, broker, server, leaseUUID, validToken := streamEventsTestEnv(t)

	conn, _ := wsDialWithAuth(t, server.URL, leaseUUID, validToken)
	require.NotNil(t, conn)
	defer conn.Close()

	awaitSubscriber(t, broker, leaseUUID)

	// Send a binary frame strictly larger than the read limit. The server's
	// read pump should trip ErrReadLimit, and gorilla auto-sends a 1009
	// CloseMessageTooBig frame back to the client.
	payload := make([]byte, wsDefaultMaxMessageSize+1)
	require.NoError(t, conn.WriteMessage(websocket.BinaryMessage, payload))

	// Asserting the close code (not just slot teardown) proves the read
	// limit actually fired — a future regression that closed the connection
	// for an unrelated reason would otherwise still pass this test.
	_, _, err := conn.ReadMessage()
	require.Error(t, err)
	var closeErr *websocket.CloseError
	require.ErrorAs(t, err, &closeErr)
	assert.Equal(t, websocket.CloseMessageTooBig, closeErr.Code,
		"expected server to send 1009 CloseMessageTooBig on oversized frame")

	// And the subscription slot must be freed so a reconnect can succeed.
	require.Eventually(t, func() bool {
		return broker.subscriberCount(leaseUUID) == 0
	}, 2*time.Second, 5*time.Millisecond, "expected server to unsubscribe after oversized frame")
}

func TestStreamLeaseEvents_AllowsExactlyMaxFrame(t *testing.T) {
	// Boundary check: gorilla rejects messages STRICTLY larger than the read
	// limit. A frame of exactly wsDefaultMaxMessageSize bytes must NOT trip
	// the limit. This locks the boundary against off-by-one regressions in
	// either fred or gorilla.
	_, broker, server, leaseUUID, validToken := streamEventsTestEnv(t)

	conn, _ := wsDialWithAuth(t, server.URL, leaseUUID, validToken)
	require.NotNil(t, conn)
	defer conn.Close()

	awaitSubscriber(t, broker, leaseUUID)

	payload := make([]byte, wsDefaultMaxMessageSize)
	require.NoError(t, conn.WriteMessage(websocket.BinaryMessage, payload))

	// Subscription must still be alive: publishing an event and reading it
	// back proves the read pump did not trip and the server is still serving.
	broker.Publish(testEvent(leaseUUID, backend.ProvisionStatusReady))
	var event backend.LeaseStatusEvent
	require.NoError(t, conn.ReadJSON(&event))
	assert.Equal(t, leaseUUID, event.LeaseUUID)
	assert.Equal(t, backend.ProvisionStatusReady, event.Status)
}

func TestStreamLeaseEvents_MaxLifetimeForcesReconnect(t *testing.T) {
	// The server caps the total lifetime of a single subscription so a tenant
	// cannot hold a per-lease slot indefinitely. On expiry the server sends a
	// CloseTryAgainLater frame and the client must reconnect.
	h, broker, server, leaseUUID, validToken := streamEventsTestEnv(t)
	h.wsMaxConnLifetime = 100 * time.Millisecond

	conn, _ := wsDialWithAuth(t, server.URL, leaseUUID, validToken)
	require.NotNil(t, conn)
	defer conn.Close()

	awaitSubscriber(t, broker, leaseUUID)

	// Read should fail with a CloseTryAgainLater frame after the configured
	// max lifetime.
	_, _, err := conn.ReadMessage()
	require.Error(t, err)
	var closeErr *websocket.CloseError
	require.ErrorAs(t, err, &closeErr)
	assert.Equal(t, websocket.CloseTryAgainLater, closeErr.Code)
	assert.Equal(t, "max connection lifetime reached", closeErr.Text)

	// Subscription slot should be freed so a reconnect can succeed.
	require.Eventually(t, func() bool {
		return broker.subscriberCount(leaseUUID) == 0
	}, 2*time.Second, 5*time.Millisecond, "expected server to unsubscribe after lifetime expiry")
}

func TestStreamLeaseEvents_DeliversEventBeforeLifetimeExpiry(t *testing.T) {
	// Regression guard: events published before the lifetime timer fires
	// must still be delivered to the client. Catches a future change that
	// reorders the for-select to check the lifetime timer before draining
	// the broker channel, or splits the pumps in a way that races teardown
	// against in-flight events.
	h, broker, server, leaseUUID, validToken := streamEventsTestEnv(t)
	// 2s lifetime: long enough that dial → handshake → awaitSubscriber →
	// Publish → ReadJSON completes well before expiry even on a slow CI
	// runner (awaitSubscriber alone can poll for up to 2s in the worst case,
	// but in practice resolves in microseconds).
	h.wsMaxConnLifetime = 2 * time.Second

	conn, _ := wsDialWithAuth(t, server.URL, leaseUUID, validToken)
	require.NotNil(t, conn)
	defer conn.Close()

	awaitSubscriber(t, broker, leaseUUID)

	// Publish well before the lifetime expires. The event must arrive first;
	// the close frame follows after expiry.
	broker.Publish(testEvent(leaseUUID, backend.ProvisionStatusReady))

	var event backend.LeaseStatusEvent
	require.NoError(t, conn.ReadJSON(&event), "expected event before lifetime expiry")
	assert.Equal(t, leaseUUID, event.LeaseUUID)
	assert.Equal(t, backend.ProvisionStatusReady, event.Status)

	// Then the close frame.
	_, _, err := conn.ReadMessage()
	require.Error(t, err)
	var closeErr *websocket.CloseError
	require.ErrorAs(t, err, &closeErr)
	assert.Equal(t, websocket.CloseTryAgainLater, closeErr.Code)
}
