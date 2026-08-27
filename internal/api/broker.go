package api

import (
	"errors"
	"log/slog"
	"sync"

	"github.com/manifest-network/fred/internal/backend"
)

const (
	eventChannelBuffer = 16
	// transitionEventBuffer leaves one subscriber slot for an accepted start
	// event, so draining a full gate still preserves its newest terminal event.
	transitionEventBuffer = eventChannelBuffer - 1

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

	// transitionMu protects the short-lived per-lease gates used by
	// DispatchWithOrderedStart. A backend is allowed to post its asynchronous
	// completion before its Restart/Update response reaches Fred. While one of
	// those dispatches is unresolved, Publish appends same-lease callbacks here
	// instead of blocking the callback response (which could deadlock a remote
	// backend waiting to return its acceptance response).
	transitionMu sync.Mutex
	transitions  map[string]*eventTransition
}

type eventTransition struct {
	// dispatch serializes overlapping tenant requests for one lease at the event
	// boundary. The backend remains the authority that accepts or refuses the
	// second operation; serialization only makes their subscriber-visible start
	// and completion windows non-overlapping.
	dispatch sync.Mutex
	refs     int
	active   bool
	pending  []backend.LeaseStatusEvent
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
		transitions: make(map[string]*eventTransition),
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
	b.transitionMu.Lock()
	defer b.transitionMu.Unlock()

	if transition := b.transitions[event.LeaseUUID]; transition != nil && transition.active {
		if len(transition.pending) < transitionEventBuffer {
			transition.pending = append(transition.pending, event)
			return
		}
		// Match the broker's best-effort bounded-delivery contract while
		// retaining the newest observed state. A buggy authenticated backend
		// cannot grow memory for the duration of a slow dispatch or make gate
		// drain time unbounded.
		copy(transition.pending, transition.pending[1:])
		transition.pending[len(transition.pending)-1] = event
		slog.Debug("dropped oldest event while maintenance transition was unresolved",
			"lease_uuid", event.LeaseUUID,
			"status", event.Status,
		)
		return
	}
	b.publishDirect(event)
}

// DispatchWithOrderedStart calls dispatch while holding a nonblocking
// subscriber-order gate for start.LeaseUUID. If dispatch succeeds, start is
// delivered before every callback event that arrived during dispatch. If it
// fails synchronously, no start event is manufactured and any independently
// valid callback observations are released in their original order.
//
// Publish never waits for dispatch: it only appends to the active gate under a
// short mutex. This matters for a remote backend that posts a callback and waits
// for Fred's HTTP response before returning its Restart/Update acceptance.
// Concurrent dispatches for the same lease are serialized; other leases are
// independent. The registry entry is reference-counted and removed after the
// last waiter, so tenant churn cannot grow it without bound.
func (b *EventBroker) DispatchWithOrderedStart(
	start backend.LeaseStatusEvent,
	dispatch func() error,
) error {
	transition := b.acquireTransition(start.LeaseUUID)
	transition.dispatch.Lock()
	accepted := false
	defer func() {
		// Keep transitionMu held across direct fan-out. A concurrent Publish can
		// therefore neither overtake the accepted start nor slip between queued
		// callbacks while the gate is being drained. This defer also runs during
		// panic unwinding: queued independent observations are released, no start
		// is invented, and the registry cannot remain permanently active.
		b.transitionMu.Lock()
		pending := transition.pending
		transition.pending = nil
		transition.active = false
		if accepted {
			b.publishDirect(start)
		}
		for _, event := range pending {
			b.publishDirect(event)
		}
		b.transitionMu.Unlock()

		transition.dispatch.Unlock()
		b.releaseTransition(start.LeaseUUID, transition)
	}()

	b.transitionMu.Lock()
	transition.active = true
	b.transitionMu.Unlock()

	err := dispatch()
	accepted = err == nil
	return err
}

func (b *EventBroker) acquireTransition(leaseUUID string) *eventTransition {
	b.transitionMu.Lock()
	defer b.transitionMu.Unlock()

	transition := b.transitions[leaseUUID]
	if transition == nil {
		transition = &eventTransition{}
		b.transitions[leaseUUID] = transition
	}
	transition.refs++
	return transition
}

func (b *EventBroker) releaseTransition(leaseUUID string, transition *eventTransition) {
	b.transitionMu.Lock()
	defer b.transitionMu.Unlock()

	transition.refs--
	if transition.refs == 0 && !transition.active {
		delete(b.transitions, leaseUUID)
	}
}

// publishDirect performs the existing nonblocking fan-out. Callers that
// participate in transition ordering hold transitionMu before entering it.
func (b *EventBroker) publishDirect(event backend.LeaseStatusEvent) {
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
