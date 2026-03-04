package api

import (
	"errors"
	"log/slog"
	"sync"

	"github.com/manifest-network/fred/internal/backend"
)

const (
	eventChannelBuffer = 16

	// DefaultMaxSubscriptionsPerLease is the maximum number of concurrent
	// WebSocket subscriptions allowed per lease UUID.
	DefaultMaxSubscriptionsPerLease = 10

	// DefaultMaxTotalSubscriptions is the maximum number of concurrent
	// WebSocket subscriptions allowed globally across all leases.
	DefaultMaxTotalSubscriptions = 1000
)

// ErrTooManySubscriptions is returned when a subscription limit is reached.
var ErrTooManySubscriptions = errors.New("too many subscriptions")

// EventBroker manages per-lease event client subscriptions with non-blocking fan-out.
// Slow clients drop events; they can re-fetch via REST.
type EventBroker struct {
	mu      sync.RWMutex
	clients map[string]map[chan backend.LeaseStatusEvent]struct{} // leaseUUID → set of channels
	closed  bool
	total   int // total subscriptions across all leases

	maxPerLease int
	maxTotal    int
}

// NewEventBroker creates a new event broker with default subscription limits.
func NewEventBroker() *EventBroker {
	return NewEventBrokerWithLimits(DefaultMaxSubscriptionsPerLease, DefaultMaxTotalSubscriptions)
}

// NewEventBrokerWithLimits creates a new event broker with the given subscription limits.
// maxPerLease limits concurrent subscriptions per lease UUID.
// maxTotal limits concurrent subscriptions globally across all leases.
func NewEventBrokerWithLimits(maxPerLease, maxTotal int) *EventBroker {
	return &EventBroker{
		clients:     make(map[string]map[chan backend.LeaseStatusEvent]struct{}),
		maxPerLease: maxPerLease,
		maxTotal:    maxTotal,
	}
}

// Subscribe registers a client channel for events on the given lease UUID.
// The returned channel is buffered; the caller should read from it in a loop.
// Returns nil and ErrTooManySubscriptions if a subscription limit is reached.
// Returns nil and nil error if the broker has been closed.
func (b *EventBroker) Subscribe(leaseUUID string) (<-chan backend.LeaseStatusEvent, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return nil, nil
	}

	if b.total >= b.maxTotal {
		slog.Warn("global subscription limit reached",
			"limit", b.maxTotal,
			"lease_uuid", leaseUUID,
		)
		return nil, ErrTooManySubscriptions
	}
	if subs := b.clients[leaseUUID]; len(subs) >= b.maxPerLease {
		slog.Warn("per-lease subscription limit reached",
			"limit", b.maxPerLease,
			"lease_uuid", leaseUUID,
			"current", len(subs),
		)
		return nil, ErrTooManySubscriptions
	}

	ch := make(chan backend.LeaseStatusEvent, eventChannelBuffer)
	if b.clients[leaseUUID] == nil {
		b.clients[leaseUUID] = make(map[chan backend.LeaseStatusEvent]struct{})
	}
	b.clients[leaseUUID][ch] = struct{}{}
	b.total++

	return ch, nil
}

// Unsubscribe removes a client channel. The channel is closed after removal.
func (b *EventBroker) Unsubscribe(leaseUUID string, ch <-chan backend.LeaseStatusEvent) {
	// We need to recover the underlying send channel from the receive-only one.
	// Since Subscribe created it, we stored the bidirectional channel in the map.
	// We find and remove the matching channel.
	b.mu.Lock()
	defer b.mu.Unlock()

	subs, ok := b.clients[leaseUUID]
	if !ok {
		return
	}

	for sendCh := range subs {
		// Compare by identity: the receive end of sendCh equals ch.
		if (<-chan backend.LeaseStatusEvent)(sendCh) == ch {
			delete(subs, sendCh)
			close(sendCh)
			b.total--
			break
		}
	}

	if len(subs) == 0 {
		delete(b.clients, leaseUUID)
	}
}

// Publish sends an event to all clients subscribed to the event's lease UUID.
// Non-blocking: if a client's channel is full, the event is dropped for that client.
func (b *EventBroker) Publish(event backend.LeaseStatusEvent) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	subs, ok := b.clients[event.LeaseUUID]
	if !ok {
		return
	}

	for ch := range subs {
		select {
		case ch <- event:
		default:
			slog.Debug("dropped event for slow WebSocket client",
				"lease_uuid", event.LeaseUUID,
				"status", event.Status,
			)
		}
	}
}

// SubscriberCount returns the number of active subscribers for a given lease UUID.
func (b *EventBroker) SubscriberCount(leaseUUID string) int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.clients[leaseUUID])
}

// Close closes all subscriber channels and prevents new subscriptions.
// Safe to call multiple times.
func (b *EventBroker) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return
	}
	b.closed = true

	for leaseUUID, subs := range b.clients {
		for ch := range subs {
			close(ch)
		}
		delete(b.clients, leaseUUID)
	}
	b.total = 0
}
