package api

import (
	"sync"

	"github.com/manifest-network/fred/internal/backend"
)

const sseChannelBuffer = 16

// SSEBroker manages per-lease SSE client subscriptions with non-blocking fan-out.
// Slow clients drop events; they can re-fetch via REST.
type SSEBroker struct {
	mu      sync.RWMutex
	clients map[string]map[chan backend.LeaseStatusEvent]struct{} // leaseUUID → set of channels
	closed  bool
}

// NewSSEBroker creates a new SSE broker.
func NewSSEBroker() *SSEBroker {
	return &SSEBroker{
		clients: make(map[string]map[chan backend.LeaseStatusEvent]struct{}),
	}
}

// Subscribe registers a client channel for events on the given lease UUID.
// The returned channel is buffered; the caller should read from it in a loop.
// Returns nil if the broker has been closed.
func (b *SSEBroker) Subscribe(leaseUUID string) <-chan backend.LeaseStatusEvent {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return nil
	}

	ch := make(chan backend.LeaseStatusEvent, sseChannelBuffer)
	if b.clients[leaseUUID] == nil {
		b.clients[leaseUUID] = make(map[chan backend.LeaseStatusEvent]struct{})
	}
	b.clients[leaseUUID][ch] = struct{}{}

	return ch
}

// Unsubscribe removes a client channel. The channel is closed after removal.
func (b *SSEBroker) Unsubscribe(leaseUUID string, ch <-chan backend.LeaseStatusEvent) {
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
			break
		}
	}

	if len(subs) == 0 {
		delete(b.clients, leaseUUID)
	}
}

// Publish sends an event to all clients subscribed to the event's lease UUID.
// Non-blocking: if a client's channel is full, the event is dropped for that client.
func (b *SSEBroker) Publish(event backend.LeaseStatusEvent) {
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
			// Client too slow — drop the event. They can re-fetch via REST.
		}
	}
}

// Close closes all subscriber channels and prevents new subscriptions.
// Safe to call multiple times.
func (b *SSEBroker) Close() {
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
}
