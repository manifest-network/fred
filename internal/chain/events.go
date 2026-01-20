package chain

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// LeaseEventType represents the type of lease event.
type LeaseEventType string

const (
	LeaseCreated      LeaseEventType = "lease_created"
	LeaseAcknowledged LeaseEventType = "lease_acknowledged"
	LeaseRejected     LeaseEventType = "lease_rejected"
	LeaseClosed       LeaseEventType = "lease_closed"
	LeaseExpired      LeaseEventType = "lease_expired"
)

// LeaseEvent represents a lease-related event from the chain.
type LeaseEvent struct {
	Type         LeaseEventType
	LeaseUUID    string
	ProviderUUID string
	Tenant       string
}

// EventSubscriber handles WebSocket subscription to CometBFT events.
type EventSubscriber struct {
	url          string
	providerUUID string
	conn         *websocket.Conn
	events       chan LeaseEvent
	done         chan struct{}
	mu           sync.Mutex
}

// NewEventSubscriber creates a new event subscriber.
func NewEventSubscriber(url, providerUUID string) (*EventSubscriber, error) {
	return &EventSubscriber{
		url:          url,
		providerUUID: providerUUID,
		events:       make(chan LeaseEvent, 100),
		done:         make(chan struct{}),
	}, nil
}

// Events returns the channel for receiving lease events.
func (s *EventSubscriber) Events() <-chan LeaseEvent {
	return s.events
}

// Start begins listening for events. It will automatically reconnect on failure.
func (s *EventSubscriber) Start(ctx context.Context) error {
	backoff := time.Second
	maxBackoff := 60 * time.Second

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-s.done:
			return nil
		default:
		}

		// Connect and run until error
		err := s.connectAndRun(ctx)
		if err != nil {
			slog.Error("websocket error", "error", err)
		}

		// Check if we should stop
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-s.done:
			return nil
		default:
		}

		// Wait before reconnecting
		slog.Info("reconnecting websocket", "backoff", backoff)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-s.done:
			return nil
		case <-time.After(backoff):
		}

		// Exponential backoff, capped at maxBackoff
		backoff = backoff * 2
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
}

// connectAndRun connects to the websocket and processes messages until an error occurs.
func (s *EventSubscriber) connectAndRun(ctx context.Context) error {
	// Connect
	dialer := websocket.Dialer{
		HandshakeTimeout: 10 * time.Second,
	}

	conn, _, err := dialer.DialContext(ctx, s.url, nil)
	if err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}

	s.mu.Lock()
	s.conn = conn
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		if s.conn != nil {
			s.conn.Close()
			s.conn = nil
		}
		s.mu.Unlock()
	}()

	slog.Info("websocket connected", "url", s.url)

	// Set up ping/pong handlers to keep connection alive
	conn.SetPongHandler(func(string) error {
		return nil
	})

	// Subscribe to lease events for this provider
	query := fmt.Sprintf("tm.event='Tx' AND lease_created.provider_uuid='%s'", s.providerUUID)
	if err := s.subscribe(conn, query); err != nil {
		return fmt.Errorf("failed to subscribe: %w", err)
	}

	slog.Info("subscribed to lease events", "provider_uuid", s.providerUUID)

	// Start ping ticker to keep connection alive
	pingTicker := time.NewTicker(30 * time.Second)
	defer pingTicker.Stop()

	// Channel to signal read errors
	readErr := make(chan error, 1)

	// Read messages in a goroutine
	go func() {
		for {
			_, message, err := conn.ReadMessage()
			if err != nil {
				readErr <- err
				return
			}
			s.handleMessage(message)
		}
	}()

	// Main loop: handle pings and wait for errors
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-s.done:
			return nil
		case err := <-readErr:
			return fmt.Errorf("read error: %w", err)
		case <-pingTicker.C:
			if err := conn.WriteControl(websocket.PingMessage, []byte{}, time.Now().Add(10*time.Second)); err != nil {
				return fmt.Errorf("ping error: %w", err)
			}
		}
	}
}

// subscribe sends a subscription request.
func (s *EventSubscriber) subscribe(conn *websocket.Conn, query string) error {
	req := jsonRPCRequest{
		JSONRPC: "2.0",
		Method:  "subscribe",
		ID:      1,
		Params: map[string]string{
			"query": query,
		},
	}

	return conn.WriteJSON(req)
}

// handleMessage processes a received WebSocket message.
func (s *EventSubscriber) handleMessage(message []byte) {
	var resp jsonRPCResponse
	if err := json.Unmarshal(message, &resp); err != nil {
		slog.Debug("failed to unmarshal message", "error", err)
		return
	}

	if resp.Result == nil {
		return
	}

	// Parse the result to extract events
	result, ok := resp.Result.(map[string]interface{})
	if !ok {
		return
	}

	events, ok := result["events"].(map[string]interface{})
	if !ok {
		return
	}

	// Extract lease events
	event := s.parseLeaseEvent(events)
	if event != nil {
		select {
		case s.events <- *event:
		default:
			slog.Warn("event channel full, dropping event")
		}
	}
}

// parseLeaseEvent extracts a LeaseEvent from CometBFT events.
func (s *EventSubscriber) parseLeaseEvent(events map[string]interface{}) *LeaseEvent {
	// Check for different event types
	eventTypes := []LeaseEventType{
		LeaseCreated,
		LeaseAcknowledged,
		LeaseRejected,
		LeaseClosed,
		LeaseExpired,
	}

	for _, eventType := range eventTypes {
		prefix := string(eventType)

		leaseUUID := getEventAttribute(events, prefix+".lease_uuid")
		if leaseUUID == "" {
			continue
		}

		providerUUID := getEventAttribute(events, prefix+".provider_uuid")
		if providerUUID != s.providerUUID {
			continue
		}

		tenant := getEventAttribute(events, prefix+".tenant")

		slog.Info("received lease event",
			"type", eventType,
			"lease_uuid", leaseUUID,
			"provider_uuid", providerUUID,
			"tenant", tenant,
		)

		return &LeaseEvent{
			Type:         eventType,
			LeaseUUID:    leaseUUID,
			ProviderUUID: providerUUID,
			Tenant:       tenant,
		}
	}

	return nil
}

// getEventAttribute extracts an attribute value from events.
func getEventAttribute(events map[string]interface{}, key string) string {
	if values, ok := events[key].([]interface{}); ok && len(values) > 0 {
		if str, ok := values[0].(string); ok {
			return str
		}
	}
	return ""
}

// Close shuts down the event subscriber.
func (s *EventSubscriber) Close() {
	close(s.done)
	s.mu.Lock()
	if s.conn != nil {
		s.conn.Close()
	}
	s.mu.Unlock()
}

// JSON-RPC types for CometBFT WebSocket.
type jsonRPCRequest struct {
	JSONRPC string            `json:"jsonrpc"`
	Method  string            `json:"method"`
	ID      int               `json:"id"`
	Params  map[string]string `json:"params"`
}

type jsonRPCResponse struct {
	JSONRPC string      `json:"jsonrpc"`
	ID      int         `json:"id"`
	Result  interface{} `json:"result"`
	Error   *rpcError   `json:"error,omitempty"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}
