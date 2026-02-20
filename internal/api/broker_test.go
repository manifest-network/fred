package api

import (
	"context"
	"net/http"
	"net/http/httptest"
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
	ch := broker.Subscribe("lease-1")

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
	ch1 := broker.Subscribe("lease-1")
	ch2 := broker.Subscribe("lease-1")

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
	ch := broker.Subscribe("lease-1")

	broker.Unsubscribe("lease-1", ch)

	// Publish after unsubscribe — should not be received (channel is closed).
	broker.Publish(testEvent("lease-1", backend.ProvisionStatusReady))

	// Channel should be closed.
	_, ok := <-ch
	assert.False(t, ok, "expected channel to be closed after unsubscribe")
}

func TestEventBroker_SlowClientDropsEvents(t *testing.T) {
	broker := NewEventBroker()
	ch := broker.Subscribe("lease-1")

	// Fill the buffer (eventChannelBuffer = 16).
	for range eventChannelBuffer+5 {
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
	ch1 := broker.Subscribe("lease-1")
	ch2 := broker.Subscribe("lease-2")

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
	ch1 := broker.Subscribe("lease-1")
	ch2 := broker.Subscribe("lease-2")

	broker.Close()

	// All channels should be closed.
	_, ok1 := <-ch1
	assert.False(t, ok1, "expected ch1 to be closed after broker.Close()")
	_, ok2 := <-ch2
	assert.False(t, ok2, "expected ch2 to be closed after broker.Close()")

	// Subscribe after close returns nil.
	ch3 := broker.Subscribe("lease-3")
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
		client:       chainClient,
		eventBroker:  broker,
		wsUpgrader:   websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }},
		providerUUID: providerUUID,
		bech32Prefix: "manifest",
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
	go func() {
		time.Sleep(50 * time.Millisecond)
		broker.Publish(backend.LeaseStatusEvent{
			LeaseUUID: leaseUUID,
			Status:    backend.ProvisionStatusReady,
			Timestamp: time.Now(),
		})
	}()

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

	// Publish an event after a short delay to ensure the client is subscribed.
	go func() {
		time.Sleep(100 * time.Millisecond)
		broker.Publish(backend.LeaseStatusEvent{
			LeaseUUID: leaseUUID,
			Status:    backend.ProvisionStatusReady,
			Timestamp: time.Now(),
		})
	}()

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

	// Publish an event after 200ms — well beyond a hypothetical 100ms timeout.
	go func() {
		time.Sleep(200 * time.Millisecond)
		broker.Publish(backend.LeaseStatusEvent{
			LeaseUUID: leaseUUID,
			Status:    backend.ProvisionStatusReady,
			Timestamp: time.Now(),
		})
	}()

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
	go func() {
		time.Sleep(100 * time.Millisecond)
		broker.Publish(backend.LeaseStatusEvent{
			LeaseUUID: leaseUUID,
			Status:    backend.ProvisionStatusReady,
			Timestamp: time.Now(),
		})
	}()

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

	// Give the server goroutine time to subscribe, then close the broker.
	time.Sleep(100 * time.Millisecond)

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

	// Give the server goroutine time to subscribe.
	time.Sleep(100 * time.Millisecond)

	// Client sends close frame.
	_ = conn.WriteMessage(websocket.CloseMessage,
		websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
	conn.Close()

	// Give server time to process the close and unsubscribe.
	time.Sleep(100 * time.Millisecond)

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

	go func() {
		time.Sleep(50 * time.Millisecond)
		for _, s := range statuses {
			broker.Publish(backend.LeaseStatusEvent{
				LeaseUUID: leaseUUID,
				Status:    s,
				Timestamp: time.Now(),
			})
		}
	}()

	for _, expected := range statuses {
		var event backend.LeaseStatusEvent
		err := conn.ReadJSON(&event)
		require.NoError(t, err)
		assert.Equal(t, leaseUUID, event.LeaseUUID)
		assert.Equal(t, expected, event.Status, "expected status %s", expected)
	}
}
