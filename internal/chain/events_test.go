package chain

import (
	"encoding/json"
	"testing"
	"time"
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
			if err != nil {
				t.Fatalf("NewEventSubscriber() error = %v", err)
			}
			if sub == nil {
				t.Fatal("NewEventSubscriber() returned nil")
			}
			if sub.url != tt.url {
				t.Errorf("url = %q, want %q", sub.url, tt.url)
			}
			if sub.providerUUID != tt.providerUUID {
				t.Errorf("providerUUID = %q, want %q", sub.providerUUID, tt.providerUUID)
			}
			if sub.pingInterval != tt.wantPing {
				t.Errorf("pingInterval = %v, want %v", sub.pingInterval, tt.wantPing)
			}
			if sub.reconnectInitial != tt.wantInitial {
				t.Errorf("reconnectInitial = %v, want %v", sub.reconnectInitial, tt.wantInitial)
			}
			if sub.reconnectMax != tt.wantMax {
				t.Errorf("reconnectMax = %v, want %v", sub.reconnectMax, tt.wantMax)
			}
			if sub.subscribers == nil {
				t.Error("subscribers map is nil")
			}
			if sub.done == nil {
				t.Error("done channel is nil")
			}
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
			if err == nil {
				t.Errorf("NewEventSubscriber() should reject invalid UUID %q", tt.providerUUID)
			}
		})
	}
}

func TestEventSubscriber_Subscribe(t *testing.T) {
	sub, err := NewEventSubscriber(EventSubscriberConfig{
		URL:          "ws://localhost:26657/websocket",
		ProviderUUID: testProviderUUID,
	})
	if err != nil {
		t.Fatalf("NewEventSubscriber() error = %v", err)
	}

	ch := sub.Subscribe()
	if ch == nil {
		t.Error("Subscribe() returned nil channel")
	}

	// Should be able to unsubscribe
	sub.Unsubscribe(ch)

	// Multiple subscriptions should work
	ch1 := sub.Subscribe()
	ch2 := sub.Subscribe()
	if ch1 == nil || ch2 == nil {
		t.Error("Subscribe() returned nil channel")
	}
	if ch1 == ch2 {
		t.Error("Subscribe() should return different channels")
	}

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
			if got != tt.want {
				t.Errorf("getEventAttribute() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestEventSubscriber_ParseLeaseEvent(t *testing.T) {
	sub, err := NewEventSubscriber(EventSubscriberConfig{
		URL:          "ws://localhost:26657/websocket",
		ProviderUUID: testProviderUUID,
	})
	if err != nil {
		t.Fatalf("NewEventSubscriber() error = %v", err)
	}

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
				if got != nil {
					t.Errorf("parseLeaseEvent() = %v, want nil", got)
				}
				return
			}
			if got == nil {
				t.Fatal("parseLeaseEvent() = nil, want non-nil")
			}
			if got.Type != tt.want.Type {
				t.Errorf("Type = %v, want %v", got.Type, tt.want.Type)
			}
			if got.LeaseUUID != tt.want.LeaseUUID {
				t.Errorf("LeaseUUID = %q, want %q", got.LeaseUUID, tt.want.LeaseUUID)
			}
			if got.ProviderUUID != tt.want.ProviderUUID {
				t.Errorf("ProviderUUID = %q, want %q", got.ProviderUUID, tt.want.ProviderUUID)
			}
			if got.Tenant != tt.want.Tenant {
				t.Errorf("Tenant = %q, want %q", got.Tenant, tt.want.Tenant)
			}
		})
	}
}

func TestEventSubscriber_HandleMessage(t *testing.T) {
	sub, err := NewEventSubscriber(EventSubscriberConfig{
		URL:          "ws://localhost:26657/websocket",
		ProviderUUID: testProviderUUID,
	})
	if err != nil {
		t.Fatalf("NewEventSubscriber() error = %v", err)
	}

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
				if !tt.expectEvent {
					t.Error("handleMessage() emitted event when none expected")
				}
			default:
				if tt.expectEvent {
					t.Error("handleMessage() did not emit expected event")
				}
			}
		})
	}
}

func TestEventSubscriber_TrackInvalidMessage(t *testing.T) {
	sub, err := NewEventSubscriber(EventSubscriberConfig{
		URL:          "ws://localhost:26657/websocket",
		ProviderUUID: testProviderUUID,
	})
	if err != nil {
		t.Fatalf("NewEventSubscriber() error = %v", err)
	}

	// Track multiple invalid messages
	for range 15 {
		sub.trackInvalidMessage("test_error", nil)
	}

	sub.mu.Lock()
	count := sub.invalidMsgCount
	sub.mu.Unlock()

	if count != 15 {
		t.Errorf("invalidMsgCount = %d, want 15", count)
	}
}

func TestEventSubscriber_Close(t *testing.T) {
	sub, err := NewEventSubscriber(EventSubscriberConfig{
		URL:          "ws://localhost:26657/websocket",
		ProviderUUID: testProviderUUID,
	})
	if err != nil {
		t.Fatalf("NewEventSubscriber() error = %v", err)
	}

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
			if string(tt.eventType) != tt.want {
				t.Errorf("LeaseEventType = %q, want %q", tt.eventType, tt.want)
			}
		})
	}
}
