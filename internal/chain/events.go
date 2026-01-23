package chain

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand/v2"
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
	LeaseAutoClosed   LeaseEventType = "lease_auto_closed" // Credit exhaustion (any provider)
)

const (
	// eventChannelCapacity is the buffer size for the lease events channel.
	// If the channel fills up, events will be dropped with a warning log.
	// Increase this value if you observe "event channel full" warnings.
	eventChannelCapacity = 100

	// invalidMsgThreshold is the count at which invalid message logging escalates to warning.
	invalidMsgThreshold = 10

	// readGoroutineCleanupTimeout is how long to wait for the read goroutine to finish during cleanup.
	readGoroutineCleanupTimeout = 5 * time.Second

	// backoffJitterDivisor controls the jitter range (1/N of backoff duration).
	// A value of 4 means jitter is 0-25% of the backoff duration.
	backoffJitterDivisor = 4
)

// LeaseEvent represents a lease-related event from the chain.
type LeaseEvent struct {
	Type         LeaseEventType
	LeaseUUID    string
	ProviderUUID string
	Tenant       string
}

// EventSubscriber handles WebSocket subscription to CometBFT events.
// It supports multiple consumers via Subscribe() - each subscriber gets
// its own channel and receives all events (fan-out pattern).
type EventSubscriber struct {
	url              string
	providerUUID     string
	pingInterval     time.Duration
	reconnectInitial time.Duration
	reconnectMax     time.Duration
	conn             *websocket.Conn
	done             chan struct{}
	mu               sync.Mutex

	// Fan-out: multiple subscribers each with their own channel
	subscribers   map[chan LeaseEvent]struct{}
	subscribersMu sync.RWMutex
	nextSubID     int // For logging/debugging

	// Track invalid messages for escalated logging
	invalidMsgCount    int
	lastInvalidMsgTime time.Time
}

// EventSubscriberConfig holds configuration for the event subscriber.
type EventSubscriberConfig struct {
	URL              string
	ProviderUUID     string
	PingInterval     time.Duration
	ReconnectInitial time.Duration
	ReconnectMax     time.Duration
}

// NewEventSubscriber creates a new event subscriber.
func NewEventSubscriber(cfg EventSubscriberConfig) (*EventSubscriber, error) {
	// Apply defaults
	pingInterval := cfg.PingInterval
	if pingInterval == 0 {
		pingInterval = 30 * time.Second
	}
	reconnectInitial := cfg.ReconnectInitial
	if reconnectInitial == 0 {
		reconnectInitial = time.Second
	}
	reconnectMax := cfg.ReconnectMax
	if reconnectMax == 0 {
		reconnectMax = 60 * time.Second
	}

	return &EventSubscriber{
		url:              cfg.URL,
		providerUUID:     cfg.ProviderUUID,
		pingInterval:     pingInterval,
		reconnectInitial: reconnectInitial,
		reconnectMax:     reconnectMax,
		done:             make(chan struct{}),
		subscribers:      make(map[chan LeaseEvent]struct{}),
	}, nil
}

// Subscribe creates a new subscription and returns a channel that receives all events.
// Each subscriber gets its own buffered channel. Call Unsubscribe when done to avoid leaks.
// Returns a bidirectional channel so it can be passed to Unsubscribe for cleanup.
func (s *EventSubscriber) Subscribe() chan LeaseEvent {
	ch := make(chan LeaseEvent, eventChannelCapacity)

	s.subscribersMu.Lock()
	s.subscribers[ch] = struct{}{}
	s.nextSubID++
	subID := s.nextSubID
	s.subscribersMu.Unlock()

	slog.Debug("new event subscriber registered", "subscriber_id", subID)
	return ch
}

// Unsubscribe removes a subscription and closes its channel.
// Safe to call multiple times or with a nil channel.
func (s *EventSubscriber) Unsubscribe(ch chan LeaseEvent) {
	if ch == nil {
		return
	}

	s.subscribersMu.Lock()
	defer s.subscribersMu.Unlock()

	if _, exists := s.subscribers[ch]; exists {
		delete(s.subscribers, ch)
		close(ch)
		slog.Debug("event subscriber unregistered")
	}
}

// broadcast sends an event to all subscribers. Non-blocking: if a subscriber's
// channel is full, the event is dropped for that subscriber with a warning.
func (s *EventSubscriber) broadcast(event LeaseEvent) {
	s.subscribersMu.RLock()
	defer s.subscribersMu.RUnlock()

	for ch := range s.subscribers {
		select {
		case ch <- event:
		default:
			slog.Warn("subscriber channel full, dropping event",
				"event_type", event.Type,
				"lease_uuid", event.LeaseUUID,
			)
		}
	}
}

// Start begins listening for events. It will automatically reconnect on failure.
func (s *EventSubscriber) Start(ctx context.Context) error {
	backoff := s.reconnectInitial

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

		// Exponential backoff with jitter, capped at max
		backoff = backoff * 2
		if backoff > s.reconnectMax {
			backoff = s.reconnectMax
		}
		// Add jitter: random value between 0 and 25% of backoff to prevent thundering herd
		jitter := time.Duration(rand.Int64N(int64(backoff) / backoffJitterDivisor))
		backoff = backoff + jitter
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

	// Cleanup function to close connection and wait for read goroutine
	// We declare readDone here so we can reference it in the defer
	var readDone chan struct{}
	defer func() {
		// Close connection first to unblock any pending ReadMessage
		s.mu.Lock()
		if s.conn != nil {
			if err := s.conn.Close(); err != nil {
				slog.Debug("error closing websocket connection", "error", err)
			}
			s.conn = nil
		}
		s.mu.Unlock()

		// Wait for read goroutine to finish (if it was started)
		if readDone != nil {
			select {
			case <-readDone:
			case <-time.After(readGoroutineCleanupTimeout):
				slog.Warn("timeout waiting for read goroutine to finish")
			}
		}
	}()

	slog.Info("websocket connected", "url", s.url)

	// Set up ping/pong handlers to keep connection alive
	conn.SetPongHandler(func(string) error {
		return nil
	})

	// Subscribe to lease events for this provider
	query := fmt.Sprintf("tm.event='Tx' AND lease_created.provider_uuid='%s'", s.providerUUID)
	if err := s.subscribe(conn, query, 1); err != nil {
		return fmt.Errorf("failed to subscribe to provider events: %w", err)
	}
	slog.Info("subscribed to provider lease events", "provider_uuid", s.providerUUID)

	// Subscribe to ALL lease_auto_closed events (any provider) to detect cross-provider credit depletion
	autoCloseQuery := "tm.event='Tx' AND lease_auto_closed.reason='credit_exhausted'"
	if err := s.subscribe(conn, autoCloseQuery, 2); err != nil {
		return fmt.Errorf("failed to subscribe to auto-close events: %w", err)
	}
	slog.Info("subscribed to lease auto-close events")

	// Start ping ticker to keep connection alive
	pingTicker := time.NewTicker(s.pingInterval)
	defer pingTicker.Stop()

	// Channel to signal read errors and completion
	readErr := make(chan error, 1)
	readDone = make(chan struct{})

	// Read messages in a goroutine
	go func() {
		defer close(readDone)
		for {
			_, message, err := conn.ReadMessage()
			if err != nil {
				// Only send error if channel is not full (non-blocking)
				select {
				case readErr <- err:
				default:
				}
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
func (s *EventSubscriber) subscribe(conn *websocket.Conn, query string, id int) error {
	req := jsonRPCRequest{
		JSONRPC: "2.0",
		Method:  "subscribe",
		ID:      id,
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
		s.trackInvalidMessage("json_unmarshal_error", err)
		return
	}

	// Check for RPC errors
	if resp.Error != nil {
		slog.Warn("received RPC error",
			"code", resp.Error.Code,
			"message", resp.Error.Message,
		)
		return
	}

	if len(resp.Result) == 0 {
		// This is normal for subscription confirmations
		return
	}

	// Parse the result to extract events
	var result txEventResult
	if err := json.Unmarshal(resp.Result, &result); err != nil {
		s.trackInvalidMessage("result_unmarshal_error", err)
		return
	}

	if result.Events == nil {
		// This is normal - not all messages contain events
		return
	}

	// Reset invalid message counter on successful event processing
	s.mu.Lock()
	s.invalidMsgCount = 0
	s.mu.Unlock()

	// Extract lease events and broadcast to all subscribers
	event := s.parseLeaseEvent(result.Events)
	if event != nil {
		s.broadcast(*event)
	}
}

// trackInvalidMessage tracks invalid messages and escalates logging if threshold exceeded.
func (s *EventSubscriber) trackInvalidMessage(reason string, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.invalidMsgCount++
	s.lastInvalidMsgTime = time.Now()

	// Build log attributes
	attrs := []interface{}{"reason", reason}
	if err != nil {
		attrs = append(attrs, "error", err)
	}

	switch {
	case s.invalidMsgCount == invalidMsgThreshold:
		// Escalate to warning at threshold
		slog.Warn("high rate of invalid WebSocket messages", append(attrs, "count", s.invalidMsgCount)...)
	case s.invalidMsgCount < invalidMsgThreshold:
		slog.Debug("invalid WebSocket message", attrs...)
	}
	// After threshold, stop logging to avoid spam
}

// parseLeaseEvent extracts a LeaseEvent from CometBFT events.
func (s *EventSubscriber) parseLeaseEvent(events map[string][]string) *LeaseEvent {
	// Check for lease_auto_closed events first (these are not filtered by provider)
	if event := s.parseAutoClosedEvent(events); event != nil {
		return event
	}

	// Check for provider-specific event types
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

// parseAutoClosedEvent parses lease_auto_closed events (from any provider).
func (s *EventSubscriber) parseAutoClosedEvent(events map[string][]string) *LeaseEvent {
	prefix := string(LeaseAutoClosed)

	leaseUUID := getEventAttribute(events, prefix+".lease_uuid")
	if leaseUUID == "" {
		return nil
	}

	tenant := getEventAttribute(events, prefix+".tenant")
	if tenant == "" {
		return nil
	}

	providerUUID := getEventAttribute(events, prefix+".provider_uuid")
	reason := getEventAttribute(events, prefix+".reason")

	slog.Info("received lease auto-closed event",
		"lease_uuid", leaseUUID,
		"tenant", tenant,
		"provider_uuid", providerUUID,
		"reason", reason,
	)

	return &LeaseEvent{
		Type:         LeaseAutoClosed,
		LeaseUUID:    leaseUUID,
		ProviderUUID: providerUUID,
		Tenant:       tenant,
	}
}

// getEventAttribute extracts an attribute value from events.
func getEventAttribute(events map[string][]string, key string) string {
	if values, ok := events[key]; ok && len(values) > 0 {
		return values[0]
	}
	return ""
}

// Close shuts down the event subscriber and closes all subscriber channels.
func (s *EventSubscriber) Close() {
	close(s.done)

	s.mu.Lock()
	if s.conn != nil {
		if err := s.conn.Close(); err != nil {
			slog.Debug("error closing websocket connection", "error", err)
		}
	}
	s.mu.Unlock()

	// Close all subscriber channels
	s.subscribersMu.Lock()
	for ch := range s.subscribers {
		close(ch)
		delete(s.subscribers, ch)
	}
	s.subscribersMu.Unlock()
}

// JSON-RPC types for CometBFT WebSocket.
type jsonRPCRequest struct {
	JSONRPC string            `json:"jsonrpc"`
	Method  string            `json:"method"`
	ID      int               `json:"id"`
	Params  map[string]string `json:"params"`
}

type jsonRPCResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      int             `json:"id"`
	Result  json.RawMessage `json:"result"`
	Error   *rpcError       `json:"error,omitempty"`
}

// txEventResult represents the result structure for transaction events.
type txEventResult struct {
	Events map[string][]string `json:"events"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}
