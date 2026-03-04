package chain

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test UUIDs - valid RFC 4122 format
const (
	testProviderUUID  = "01234567-89ab-cdef-0123-456789abcdef"
	testProviderUUID2 = "abcdef01-2345-6789-abcd-ef0123456789"
)

func TestNewEventSubscriber(t *testing.T) {
	tests := []struct {
		name             string
		url              string
		providerUUID     string
		pingInterval     time.Duration
		reconnectInitial time.Duration
		reconnectMax     time.Duration
		wantPing         time.Duration
		wantInitial      time.Duration
		wantMax          time.Duration
	}{
		{
			name:             "with defaults",
			url:              "ws://localhost:26657/websocket",
			providerUUID:     testProviderUUID,
			pingInterval:     0,
			reconnectInitial: 0,
			reconnectMax:     0,
			wantPing:         30 * time.Second,
			wantInitial:      time.Second,
			wantMax:          60 * time.Second,
		},
		{
			name:             "with custom values",
			url:              "wss://example.com/websocket",
			providerUUID:     testProviderUUID2,
			pingInterval:     15 * time.Second,
			reconnectInitial: 2 * time.Second,
			reconnectMax:     120 * time.Second,
			wantPing:         15 * time.Second,
			wantInitial:      2 * time.Second,
			wantMax:          120 * time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sub, err := NewEventSubscriber(EventSubscriberConfig{
				URL:              tt.url,
				ProviderUUID:     tt.providerUUID,
				PingInterval:     tt.pingInterval,
				ReconnectInitial: tt.reconnectInitial,
				ReconnectMax:     tt.reconnectMax,
			})
			require.NoError(t, err)
			require.NotNil(t, sub)
			assert.Equal(t, tt.url, sub.url)
			assert.Equal(t, tt.providerUUID, sub.providerUUID)
			assert.Equal(t, tt.wantPing, sub.pingInterval)
			assert.Equal(t, tt.wantInitial, sub.reconnectInitial)
			assert.Equal(t, tt.wantMax, sub.reconnectMax)
			assert.NotNil(t, sub.subscribers)
			assert.NotNil(t, sub.done)
		})
	}
}

func TestNewEventSubscriber_InvalidUUID(t *testing.T) {
	tests := []struct {
		name         string
		providerUUID string
	}{
		{name: "empty UUID", providerUUID: ""},
		{name: "invalid format", providerUUID: "not-a-uuid"},
		{name: "too short", providerUUID: "12345678"},
		{name: "injection attempt", providerUUID: "test' OR '1'='1"},
		{name: "special characters", providerUUID: "abc123; DROP TABLE"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewEventSubscriber(EventSubscriberConfig{
				URL:          "ws://localhost:26657/websocket",
				ProviderUUID: tt.providerUUID,
			})
			assert.Error(t, err)
		})
	}
}

func TestEventSubscriber_Subscribe(t *testing.T) {
	sub, err := NewEventSubscriber(EventSubscriberConfig{
		URL:          "ws://localhost:26657/websocket",
		ProviderUUID: testProviderUUID,
	})
	require.NoError(t, err)

	ch := sub.Subscribe()
	assert.NotNil(t, ch)

	// Should be able to unsubscribe
	sub.Unsubscribe(ch)

	// Multiple subscriptions should work
	ch1 := sub.Subscribe()
	ch2 := sub.Subscribe()
	assert.NotNil(t, ch1)
	assert.NotNil(t, ch2)
	assert.False(t, ch1 == ch2)

	sub.Unsubscribe(ch1)
	sub.Unsubscribe(ch2)
}

func TestGetEventAttribute(t *testing.T) {
	tests := []struct {
		name   string
		events map[string][]string
		key    string
		want   string
	}{
		{
			name: "existing attribute",
			events: map[string][]string{
				"lease_created.lease_uuid": {"uuid-123"},
			},
			key:  "lease_created.lease_uuid",
			want: "uuid-123",
		},
		{
			name: "missing attribute",
			events: map[string][]string{
				"other.attr": {"value"},
			},
			key:  "lease_created.lease_uuid",
			want: "",
		},
		{
			name:   "empty events",
			events: map[string][]string{},
			key:    "lease_created.lease_uuid",
			want:   "",
		},
		{
			name: "empty array",
			events: map[string][]string{
				"lease_created.lease_uuid": {},
			},
			key:  "lease_created.lease_uuid",
			want: "",
		},
		{
			name: "multiple values returns first",
			events: map[string][]string{
				"lease_created.lease_uuid": {"first", "second"},
			},
			key:  "lease_created.lease_uuid",
			want: "first",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := getEventAttribute(tt.events, tt.key)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestEventSubscriber_ParseLeaseEvent(t *testing.T) {
	sub, err := NewEventSubscriber(EventSubscriberConfig{
		URL:          "ws://localhost:26657/websocket",
		ProviderUUID: testProviderUUID,
	})
	require.NoError(t, err)

	tests := []struct {
		name   string
		events map[string][]string
		want   *LeaseEvent
	}{
		{
			name: "lease_created event",
			events: map[string][]string{
				"lease_created.lease_uuid":    {"lease-uuid-1"},
				"lease_created.provider_uuid": {testProviderUUID},
				"lease_created.tenant":        {"tenant-addr"},
			},
			want: &LeaseEvent{
				Type:         LeaseCreated,
				LeaseUUID:    "lease-uuid-1",
				ProviderUUID: testProviderUUID,
				Tenant:       "tenant-addr",
			},
		},
		{
			name: "lease_acknowledged event",
			events: map[string][]string{
				"lease_acknowledged.lease_uuid":    {"lease-uuid-2"},
				"lease_acknowledged.provider_uuid": {testProviderUUID},
				"lease_acknowledged.tenant":        {"tenant-addr"},
			},
			want: &LeaseEvent{
				Type:         LeaseAcknowledged,
				LeaseUUID:    "lease-uuid-2",
				ProviderUUID: testProviderUUID,
				Tenant:       "tenant-addr",
			},
		},
		{
			name: "event for different provider - ignored",
			events: map[string][]string{
				"lease_created.lease_uuid":    {"lease-uuid-3"},
				"lease_created.provider_uuid": {"other-provider"},
				"lease_created.tenant":        {"tenant-addr"},
			},
			want: nil,
		},
		{
			name: "auto_closed event",
			events: map[string][]string{
				"lease_auto_closed.lease_uuid":    {"lease-uuid-4"},
				"lease_auto_closed.provider_uuid": {"any-provider"},
				"lease_auto_closed.tenant":        {"tenant-addr"},
				"lease_auto_closed.reason":        {"credit_exhausted"},
			},
			want: &LeaseEvent{
				Type:         LeaseAutoClosed,
				LeaseUUID:    "lease-uuid-4",
				ProviderUUID: "any-provider",
				Tenant:       "tenant-addr",
			},
		},
		{
			name:   "no relevant events",
			events: map[string][]string{},
			want:   nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := sub.parseLeaseEvent(tt.events)
			if tt.want == nil {
				assert.Nil(t, got)
				return
			}
			require.NotNil(t, got)
			assert.Equal(t, tt.want.Type, got.Type)
			assert.Equal(t, tt.want.LeaseUUID, got.LeaseUUID)
			assert.Equal(t, tt.want.ProviderUUID, got.ProviderUUID)
			assert.Equal(t, tt.want.Tenant, got.Tenant)
		})
	}
}

func TestEventSubscriber_HandleMessage(t *testing.T) {
	sub, err := NewEventSubscriber(EventSubscriberConfig{
		URL:          "ws://localhost:26657/websocket",
		ProviderUUID: testProviderUUID,
	})
	require.NoError(t, err)

	// Helper to create result JSON with events
	makeResultJSON := func(events map[string][]string) json.RawMessage {
		result := txEventResult{Events: events}
		data, _ := json.Marshal(result)
		return data
	}

	tests := []struct {
		name        string
		message     string
		expectEvent bool
	}{
		{
			name: "valid lease_created message",
			message: func() string {
				resp := jsonRPCResponse{
					JSONRPC: "2.0",
					ID:      1,
					Result: makeResultJSON(map[string][]string{
						"lease_created.lease_uuid":    {"lease-uuid-1"},
						"lease_created.provider_uuid": {testProviderUUID},
						"lease_created.tenant":        {"tenant-addr"},
					}),
				}
				data, _ := json.Marshal(resp)
				return string(data)
			}(),
			expectEvent: true,
		},
		{
			name: "subscription confirmation (empty result)",
			message: func() string {
				resp := jsonRPCResponse{
					JSONRPC: "2.0",
					ID:      1,
					Result:  nil,
				}
				data, _ := json.Marshal(resp)
				return string(data)
			}(),
			expectEvent: false,
		},
		{
			name: "rpc error",
			message: func() string {
				resp := jsonRPCResponse{
					JSONRPC: "2.0",
					ID:      1,
					Error: &rpcError{
						Code:    -1,
						Message: "some error",
					},
				}
				data, _ := json.Marshal(resp)
				return string(data)
			}(),
			expectEvent: false,
		},
		{
			name:        "invalid json",
			message:     "not json",
			expectEvent: false,
		},
	}

	// Subscribe to receive events
	events := sub.Subscribe()
	defer sub.Unsubscribe(events)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Drain any existing events
			select {
			case <-events:
			default:
			}

			sub.handleMessage([]byte(tt.message))

			// Check if event was emitted
			select {
			case <-events:
				assert.True(t, tt.expectEvent)
			default:
				assert.False(t, tt.expectEvent)
			}
		})
	}
}

func TestEventSubscriber_TrackInvalidMessage(t *testing.T) {
	sub, err := NewEventSubscriber(EventSubscriberConfig{
		URL:          "ws://localhost:26657/websocket",
		ProviderUUID: testProviderUUID,
	})
	require.NoError(t, err)

	// Track multiple invalid messages
	for range 15 {
		sub.trackInvalidMessage("test_error", nil)
	}

	sub.mu.Lock()
	count := sub.invalidMsgCount
	sub.mu.Unlock()

	assert.Equal(t, 15, count)
}

func TestEventSubscriber_Close(t *testing.T) {
	sub, err := NewEventSubscriber(EventSubscriberConfig{
		URL:          "ws://localhost:26657/websocket",
		ProviderUUID: testProviderUUID,
	})
	require.NoError(t, err)

	// Close should not panic
	sub.Close()

	// Verify done channel is closed
	select {
	case <-sub.done:
		// Expected - channel is closed
	default:
		t.Error("Close() did not close done channel")
	}
}

func TestEventSubscriber_ConnectAndRunSubscriptions(t *testing.T) {
	// Collect subscription requests sent by connectAndRun
	type subRequest struct {
		ID    int
		Query string
	}

	subscriptions := make(chan subRequest, 10)

	upgrader := websocket.Upgrader{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Logf("upgrade error: %v", err)
			return
		}
		defer conn.Close()

		for {
			_, msg, err := conn.ReadMessage()
			if err != nil {
				return
			}

			var req jsonRPCRequest
			if err := json.Unmarshal(msg, &req); err != nil {
				continue
			}

			if req.Method == "subscribe" {
				subscriptions <- subRequest{ID: req.ID, Query: req.Params["query"]}
				// Send subscription confirmation
				resp := jsonRPCResponse{JSONRPC: "2.0", ID: req.ID}
				if err := conn.WriteJSON(resp); err != nil {
					return
				}
			}
		}
	}))
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "/websocket"

	sub, err := NewEventSubscriber(EventSubscriberConfig{
		URL:          wsURL,
		ProviderUUID: testProviderUUID,
		PingInterval: time.Hour, // avoid ping interference
	})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Run connectAndRun in a goroutine
	errCh := make(chan error, 1)
	go func() {
		errCh <- sub.connectAndRun(ctx)
	}()

	// Collect all 4 subscription requests
	var subs []subRequest
	timeout := time.After(5 * time.Second)
	for len(subs) < 4 {
		select {
		case s := <-subscriptions:
			subs = append(subs, s)
		case <-timeout:
			t.Fatalf("timed out waiting for subscriptions, got %d of 4", len(subs))
		}
	}

	// Cancel context to stop connectAndRun and wait for goroutine to exit
	cancel()
	select {
	case <-errCh:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for connectAndRun to exit")
	}

	// Verify we got exactly 4 subscriptions
	require.Len(t, subs, 4)

	// Build a map by ID for easier assertion
	subByID := make(map[int]string)
	for _, s := range subs {
		subByID[s.ID] = s.Query
	}

	// Verify subscription IDs and queries
	assert.Contains(t, subByID[subscriptionIDLeaseCreated], "lease_created.provider_uuid='"+testProviderUUID+"'",
		"lease_created subscription should filter by provider UUID")
	assert.Contains(t, subByID[subscriptionIDAutoClose], "lease_auto_closed.reason='credit_exhausted'",
		"auto_close subscription should filter by credit_exhausted reason")
	assert.Contains(t, subByID[subscriptionIDLeaseClosed], "lease_closed.provider_uuid='"+testProviderUUID+"'",
		"lease_closed subscription should filter by provider UUID")
	assert.Contains(t, subByID[subscriptionIDLeaseExpired], "lease_expired.provider_uuid='"+testProviderUUID+"'",
		"lease_expired subscription should filter by provider UUID")

	// Verify all queries include tm.event='Tx'
	for id, query := range subByID {
		assert.Contains(t, query, "tm.event='Tx'", "subscription %d should filter for Tx events", id)
	}
}

func TestEventSubscriber_Start_ReconnectsOnFailure(t *testing.T) {
	// Track how many times the server accepts a WebSocket connection.
	var connectCount atomic.Int32

	upgrader := websocket.Upgrader{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		connectCount.Add(1)
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		// Close immediately to trigger a read error in connectAndRun,
		// which will cause Start() to reconnect.
		conn.Close()
	}))
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "/websocket"

	sub, err := NewEventSubscriber(EventSubscriberConfig{
		URL:              wsURL,
		ProviderUUID:     testProviderUUID,
		PingInterval:     time.Hour, // avoid interference
		ReconnectInitial: 10 * time.Millisecond,
		ReconnectMax:     50 * time.Millisecond,
	})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- sub.Start(ctx)
	}()

	// Wait for at least 3 connections (initial + 2 reconnects).
	assert.Eventually(t, func() bool {
		return connectCount.Load() >= 3
	}, 5*time.Second, 10*time.Millisecond,
		"expected at least 3 connections, got %d", connectCount.Load())

	// Shut down via context cancellation
	cancel()

	select {
	case err := <-errCh:
		assert.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for Start() to return")
	}
}

func TestEventSubscriber_Start_StopsOnClose(t *testing.T) {
	upgrader := websocket.Upgrader{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		// Read and respond to subscriptions, then hold connection open
		for {
			_, msg, err := conn.ReadMessage()
			if err != nil {
				return
			}
			var req jsonRPCRequest
			if json.Unmarshal(msg, &req) == nil && req.Method == "subscribe" {
				conn.WriteJSON(jsonRPCResponse{JSONRPC: "2.0", ID: req.ID})
			}
		}
	}))
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "/websocket"

	sub, err := NewEventSubscriber(EventSubscriberConfig{
		URL:              wsURL,
		ProviderUUID:     testProviderUUID,
		PingInterval:     time.Hour,
		ReconnectInitial: 10 * time.Millisecond,
		ReconnectMax:     50 * time.Millisecond,
	})
	require.NoError(t, err)

	ctx := context.Background()
	errCh := make(chan error, 1)
	go func() {
		errCh <- sub.Start(ctx)
	}()

	// Give it time to connect
	time.Sleep(100 * time.Millisecond)

	// Close should cause Start to return nil
	sub.Close()

	select {
	case err := <-errCh:
		assert.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for Start() to return after Close()")
	}
}

func TestLeaseEventType_String(t *testing.T) {
	tests := []struct {
		eventType LeaseEventType
		want      string
	}{
		{LeaseCreated, "lease_created"},
		{LeaseAcknowledged, "lease_acknowledged"},
		{LeaseRejected, "lease_rejected"},
		{LeaseClosed, "lease_closed"},
		{LeaseExpired, "lease_expired"},
		{LeaseAutoClosed, "lease_auto_closed"},
	}

	for _, tt := range tests {
		t.Run(string(tt.eventType), func(t *testing.T) {
			assert.Equal(t, tt.want, string(tt.eventType))
		})
	}
}
