package chain

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"

	"github.com/manifest-network/fred/internal/metrics"
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
	// DefaultEventChannelCapacity is the default buffer size for subscriber channels.
	// If the channel fills up, events will be dropped with a warning log.
	// Monitor fred_events_dropped_total metric for capacity issues.
	DefaultEventChannelCapacity = 1000

	// invalidMsgThreshold is the count at which invalid message logging escalates to warning.
	invalidMsgThreshold = 10

	// readGoroutineCleanupTimeout is how long to wait for the read goroutine to finish during cleanup.
	readGoroutineCleanupTimeout = 5 * time.Second

	// WebSocket JSON-RPC subscription IDs.
	// These are used to identify different event subscriptions in the CometBFT WebSocket.
	subscriptionIDLeaseCreated = 1 // Lease creation events for this provider
	subscriptionIDAutoClose    = 2 // Cross-provider lease auto-close events (credit exhaustion)
	subscriptionIDLeaseClosed  = 3 // Lease closure events for this provider
	subscriptionIDLeaseExpired = 4 // Lease expiry events for this provider
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
	channelCapacity  int
	conn             *websocket.Conn
	done             chan struct{}
	mu               sync.Mutex

	// Fan-out: multiple subscribers each with their own channel
	subscribers   map[chan LeaseEvent]struct{}
	subscribersMu sync.RWMutex
	nextSubID     int // For logging/debugging

	// closed is set to 1 when Close() is called to prevent races between
	// broadcast() and Close(). Using atomic to avoid lock contention.
	closed atomic.Bool

	// broadcastWg tracks in-flight broadcasts so Close() can wait for them
	// to complete before closing channels. This prevents sending to closed channels.
	broadcastWg sync.WaitGroup

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
	ChannelCapacity  int // Buffer size for subscriber channels (default: 1000)
}

// NewEventSubscriber creates a new event subscriber.
func NewEventSubscriber(cfg EventSubscriberConfig) (*EventSubscriber, error) {
	// Validate provider UUID format to prevent query injection (defense in depth)
	// The UUID is interpolated into WebSocket subscription queries, so we must ensure
	// it contains only valid UUID characters (hex digits and dashes) to prevent injection.
	if _, err := uuid.Parse(cfg.ProviderUUID); err != nil {
		return nil, fmt.Errorf("invalid provider UUID format: %w", err)
	}

	// Apply defaults using cmp.Or (returns first non-zero value)
	pingInterval := cmp.Or(cfg.PingInterval, 30*time.Second)
	reconnectInitial := cmp.Or(cfg.ReconnectInitial, time.Second)
	reconnectMax := cmp.Or(cfg.ReconnectMax, 60*time.Second)
	channelCapacity := cmp.Or(max(cfg.ChannelCapacity, 0), DefaultEventChannelCapacity)

	return &EventSubscriber{
		url:              cfg.URL,
		providerUUID:     cfg.ProviderUUID,
		pingInterval:     pingInterval,
		reconnectInitial: reconnectInitial,
		reconnectMax:     reconnectMax,
		channelCapacity:  channelCapacity,
		done:             make(chan struct{}),
		subscribers:      make(map[chan LeaseEvent]struct{}),
	}, nil
}

// Subscribe creates a new subscription and returns a channel that receives all events.
// Each subscriber gets its own buffered channel. Call Unsubscribe when done to avoid leaks.
// Returns a bidirectional channel so it can be passed to Unsubscribe for cleanup.
// Returns nil if the subscriber has been closed.
func (s *EventSubscriber) Subscribe() chan LeaseEvent {
	// Check closed state before subscribing
	if s.closed.Load() {
		slog.Warn("attempted to subscribe to closed event subscriber")
		return nil
	}

	ch := make(chan LeaseEvent, s.channelCapacity)

	s.subscribersMu.Lock()
	// Double-check closed state under lock
	if s.closed.Load() {
		s.subscribersMu.Unlock()
		slog.Warn("attempted to subscribe to closed event subscriber")
		return nil
	}
	s.subscribers[ch] = struct{}{}
	s.nextSubID++
	subID := s.nextSubID
	s.subscribersMu.Unlock()

	slog.Debug("new event subscriber registered", "subscriber_id", subID, "channel_capacity", s.channelCapacity)
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
	// CRITICAL: Add to WaitGroup FIRST, before any closed check.
	// This ensures Close().Wait() will block until we're done.
	// The sequence must be:
	//   1. broadcast() calls Add(1)
	//   2. Close() sets closed=1
	//   3. Close() calls Wait() - blocks because counter > 0
	//   4. broadcast() checks closed, sees 1, returns (defer calls Done())
	//   5. Wait() unblocks
	// If we checked closed first, there's a race where Close() could finish
	// before we call Add(), and we'd send to closed channels.
	s.broadcastWg.Add(1)
	defer s.broadcastWg.Done()

	// Check if closed - if so, return early (Done() is deferred)
	if s.closed.Load() {
		return
	}

	// Collect subscriber channels under lock, then release lock before sending.
	// This prevents deadlock if a subscriber's handler calls Unsubscribe(),
	// and ensures we don't hold the lock during potentially slow operations.
	s.subscribersMu.RLock()
	channels := slices.Collect(maps.Keys(s.subscribers))
	s.subscribersMu.RUnlock()

	// Send to all channels without holding the lock.
	// Close() waits on broadcastWg before closing channels, so that path is safe.
	// Unsubscribe() may close a channel concurrently; trySend() recovers from
	// the resulting panic (the event is for a departing subscriber anyway).
	for _, ch := range channels {
		s.trySend(ch, event)
	}
}

// trySend attempts to send an event to a subscriber channel.
// Non-blocking and panic-safe (handles closed channels gracefully).
func (s *EventSubscriber) trySend(ch chan LeaseEvent, event LeaseEvent) {
	// Recover from panic if channel was closed (defensive programming)
	defer func() {
		if r := recover(); r != nil {
			slog.Warn("recovered from send on closed channel",
				"event_type", event.Type,
				"lease_uuid", event.LeaseUUID,
			)
		}
	}()

	select {
	case ch <- event:
	default:
		metrics.EventsDroppedTotal.WithLabelValues(string(event.Type)).Inc()
		slog.Warn("subscriber channel full, dropping event",
			"event_type", event.Type,
			"lease_uuid", event.LeaseUUID,
		)
	}
}

// Start begins listening for events. It will automatically reconnect on failure.
func (s *EventSubscriber) Start(ctx context.Context) error {
	// Configure exponential backoff with jitter using cenkalti/backoff
	expBackoff := backoff.NewExponentialBackOff()
	expBackoff.InitialInterval = s.reconnectInitial
	expBackoff.MaxInterval = s.reconnectMax
	expBackoff.MaxElapsedTime = 0 // Never stop retrying
	expBackoff.RandomizationFactor = 0.25
	expBackoff.Multiplier = 2.0
	expBackoff.Reset()

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

		// Get next backoff duration (includes jitter)
		wait := expBackoff.NextBackOff()
		slog.Info("reconnecting websocket", "backoff", wait)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-s.done:
			return nil
		case <-time.After(wait):
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

	// Subscribe to lease_created events for this provider
	createdQuery := fmt.Sprintf("tm.event='Tx' AND lease_created.provider_uuid='%s'", s.providerUUID)
	if err := s.subscribe(conn, createdQuery, subscriptionIDLeaseCreated); err != nil {
		return fmt.Errorf("failed to subscribe to lease created events: %w", err)
	}
	slog.Info("subscribed to lease created events", "provider_uuid", s.providerUUID)

	// Subscribe to ALL lease_auto_closed events (any provider) to detect cross-provider credit depletion
	autoCloseQuery := "tm.event='Tx' AND lease_auto_closed.reason='credit_exhausted'"
	if err := s.subscribe(conn, autoCloseQuery, subscriptionIDAutoClose); err != nil {
		return fmt.Errorf("failed to subscribe to auto-close events: %w", err)
	}
	slog.Info("subscribed to lease auto-close events")

	// Subscribe to lease_closed events for this provider
	closedQuery := fmt.Sprintf("tm.event='Tx' AND lease_closed.provider_uuid='%s'", s.providerUUID)
	if err := s.subscribe(conn, closedQuery, subscriptionIDLeaseClosed); err != nil {
		return fmt.Errorf("failed to subscribe to lease closed events: %w", err)
	}
	slog.Info("subscribed to lease closed events", "provider_uuid", s.providerUUID)

	// Subscribe to lease_expired events for this provider
	expiredQuery := fmt.Sprintf("tm.event='Tx' AND lease_expired.provider_uuid='%s'", s.providerUUID)
	if err := s.subscribe(conn, expiredQuery, subscriptionIDLeaseExpired); err != nil {
		return fmt.Errorf("failed to subscribe to lease expired events: %w", err)
	}
	slog.Info("subscribed to lease expired events", "provider_uuid", s.providerUUID)

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

	// Extract lease events and broadcast to all subscribers.
	// A single TX can contain multiple lease operations (e.g. batch close),
	// so we must iterate all extracted events.
	for _, event := range s.parseLeaseEvents(result.Events) {
		s.broadcast(event)
	}
}

// trackInvalidMessage tracks invalid messages and escalates logging if threshold exceeded.
func (s *EventSubscriber) trackInvalidMessage(reason string, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.invalidMsgCount++
	s.lastInvalidMsgTime = time.Now()

	// Build log attributes
	attrs := []any{"reason", reason}
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

// parseLeaseEvents extracts all LeaseEvents from CometBFT events.
// A single Cosmos TX can contain multiple lease operations (e.g. batch close),
// so event attributes are parallel arrays that must be iterated in lockstep.
// By design, this returns events for only the first lease event type in the
// check order that produces results for this provider, or nil if no events
// match. CometBFT TXs typically contain only one lease event type; if multiple
// lease event types are present in the same TX payload, later types are
// intentionally ignored rather than emitted alongside the first match.
func (s *EventSubscriber) parseLeaseEvents(events map[string][]string) []LeaseEvent {
	// Check for lease_auto_closed events first (these are not filtered by provider)
	if result := s.parseAutoClosedEvents(events); len(result) > 0 {
		return result
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

		leaseUUIDs := events[prefix+".lease_uuid"]
		if len(leaseUUIDs) == 0 {
			continue
		}

		providerUUIDs := events[prefix+".provider_uuid"]
		tenants := events[prefix+".tenant"]

		var result []LeaseEvent
		for i, leaseUUID := range leaseUUIDs {
			if leaseUUID == "" {
				slog.Warn("skipping lease event: empty lease_uuid at index",
					"index", i, "event_type", eventType,
					"lease_uuid_array_len", len(leaseUUIDs),
				)
				continue
			}

			providerUUID := safeIndex(providerUUIDs, i)
			if providerUUID == "" {
				slog.Warn("skipping lease event: missing provider_uuid at index",
					"index", i, "lease_uuid", leaseUUID, "event_type", eventType,
					"provider_uuid_array_len", len(providerUUIDs),
					"lease_uuid_array_len", len(leaseUUIDs),
				)
				continue
			}
			if providerUUID != s.providerUUID {
				continue
			}

			tenant := safeIndex(tenants, i)
			if tenant == "" && i >= len(tenants) {
				slog.Warn("lease event has missing tenant at index",
					"index", i, "lease_uuid", leaseUUID, "event_type", eventType,
					"tenant_array_len", len(tenants),
					"lease_uuid_array_len", len(leaseUUIDs),
				)
			}

			slog.Info("received lease event",
				"type", eventType,
				"lease_uuid", leaseUUID,
				"provider_uuid", providerUUID,
				"tenant", tenant,
			)

			result = append(result, LeaseEvent{
				Type:         eventType,
				LeaseUUID:    leaseUUID,
				ProviderUUID: providerUUID,
				Tenant:       tenant,
			})
		}

		if len(result) > 0 {
			return result
		}
	}

	return nil
}

// parseAutoClosedEvents parses lease_auto_closed events (from any provider).
// A single TX can auto-close multiple leases (e.g. credit exhaustion).
// Events missing a tenant field are skipped with a warning log.
func (s *EventSubscriber) parseAutoClosedEvents(events map[string][]string) []LeaseEvent {
	prefix := string(LeaseAutoClosed)

	leaseUUIDs := events[prefix+".lease_uuid"]
	if len(leaseUUIDs) == 0 {
		return nil
	}

	tenants := events[prefix+".tenant"]
	providerUUIDs := events[prefix+".provider_uuid"]
	reasons := events[prefix+".reason"]

	var result []LeaseEvent
	for i, leaseUUID := range leaseUUIDs {
		if leaseUUID == "" {
			slog.Warn("skipping auto-closed event: empty lease_uuid at index",
				"index", i,
				"lease_uuid_array_len", len(leaseUUIDs),
			)
			continue
		}

		tenant := safeIndex(tenants, i)
		if tenant == "" {
			slog.Warn("skipping auto-closed event: missing tenant at index",
				"index", i, "lease_uuid", leaseUUID,
				"tenant_array_len", len(tenants),
				"lease_uuid_array_len", len(leaseUUIDs),
			)
			continue
		}

		providerUUID := safeIndex(providerUUIDs, i)
		if providerUUID == "" && i >= len(providerUUIDs) {
			slog.Warn("auto-closed event has missing provider_uuid at index",
				"index", i, "lease_uuid", leaseUUID,
				"provider_uuid_array_len", len(providerUUIDs),
				"lease_uuid_array_len", len(leaseUUIDs),
			)
		}
		reason := safeIndex(reasons, i)

		slog.Info("received lease auto-closed event",
			"lease_uuid", leaseUUID,
			"tenant", tenant,
			"provider_uuid", providerUUID,
			"reason", reason,
		)

		result = append(result, LeaseEvent{
			Type:         LeaseAutoClosed,
			LeaseUUID:    leaseUUID,
			ProviderUUID: providerUUID,
			Tenant:       tenant,
		})
	}

	return result
}

// safeIndex returns the element at index i, or "" if i is out of bounds.
// Used for safe parallel-array access when CometBFT event arrays may have
// mismatched lengths.
func safeIndex(s []string, i int) string {
	if i >= 0 && i < len(s) {
		return s[i]
	}
	return ""
}

// Close shuts down the event subscriber and closes all subscriber channels.
// Safe to call multiple times - subsequent calls are no-ops.
func (s *EventSubscriber) Close() {
	// Set closed flag first to stop any new broadcasts from starting.
	// Use CompareAndSwap to ensure Close() is idempotent.
	if !s.closed.CompareAndSwap(false, true) {
		return // Already closed
	}

	close(s.done)

	s.mu.Lock()
	if s.conn != nil {
		if err := s.conn.Close(); err != nil {
			slog.Debug("error closing websocket connection", "error", err)
		}
	}
	s.mu.Unlock()

	// Wait for any in-flight broadcasts to complete before closing channels.
	// This prevents sending to closed channels and eliminates the need for
	// panic recovery in trySend(). The closed flag ensures no NEW broadcasts
	// will call Add() after this Wait() starts.
	s.broadcastWg.Wait()

	// Now safe to close all subscriber channels - no broadcasts are in flight.
	s.subscribersMu.Lock()
	for _, ch := range slices.Collect(maps.Keys(s.subscribers)) {
		close(ch)
	}
	clear(s.subscribers)
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
