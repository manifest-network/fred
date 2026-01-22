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
// It listens on the EventSubscriber's channel and publishes to Watermill topics.
type EventBridge struct {
	subscriber *chain.EventSubscriber
	publisher  EventPublisher
}

// NewEventBridge creates a new event bridge.
func NewEventBridge(subscriber *chain.EventSubscriber, publisher EventPublisher) *EventBridge {
	return &EventBridge{
		subscriber: subscriber,
		publisher:  publisher,
	}
}

// Start begins forwarding events from the chain subscriber to Watermill.
// This should be run in a goroutine.
func (b *EventBridge) Start(ctx context.Context) error {
	slog.Info("starting event bridge")

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case event := <-b.subscriber.Events():
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
