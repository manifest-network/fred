package provisioner

import (
	"context"
	"log/slog"

	"github.com/manifest-network/fred/internal/chain"
)

// EventPublisher publishes chain events to Watermill.
type EventPublisher interface {
	PublishLeaseEvent(event chain.LeaseEvent) error
}

// EventBridge bridges chain events to Watermill via the Manager.
// It subscribes to the EventSubscriber and publishes to Watermill topics.
type EventBridge struct {
	subscriber *chain.EventSubscriber
	publisher  EventPublisher
	ready      chan struct{}
}

// NewEventBridge creates a new event bridge.
func NewEventBridge(subscriber *chain.EventSubscriber, publisher EventPublisher) *EventBridge {
	return &EventBridge{
		subscriber: subscriber,
		publisher:  publisher,
		ready:      make(chan struct{}),
	}
}

// Start begins forwarding events from the chain subscriber to Watermill.
// It must be called exactly once and should be run in a goroutine.
func (b *EventBridge) Start(ctx context.Context) error {
	slog.Info("starting event bridge")

	// Subscribe to receive events (fan-out: each subscriber gets all events)
	events := b.subscriber.Subscribe()
	defer b.subscriber.Unsubscribe(events)
	close(b.ready)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case event, ok := <-events:
			if !ok {
				// Channel closed, subscriber is shutting down
				return nil
			}
			if err := b.publisher.PublishLeaseEvent(event); err != nil {
				slog.Error("failed to publish event to watermill",
					"event_type", event.Type,
					"lease_uuid", event.LeaseUUID,
					"error", err,
				)
				// Continue processing - don't block on publish errors
			}
		}
	}
}

// Ready returns a channel that is closed when the bridge has subscribed and is
// ready to forward events.
func (b *EventBridge) Ready() <-chan struct{} {
	return b.ready
}
