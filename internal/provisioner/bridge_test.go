package provisioner

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/manifest-network/fred/internal/chain"
)

// mockEventPublisher implements EventPublisher for testing.
type mockEventPublisher struct {
	mu         sync.Mutex
	events     []chain.LeaseEvent
	publishErr error
}

func (m *mockEventPublisher) PublishLeaseEvent(event chain.LeaseEvent) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.publishErr != nil {
		return m.publishErr
	}
	m.events = append(m.events, event)
	return nil
}

func (m *mockEventPublisher) getEvents() []chain.LeaseEvent {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]chain.LeaseEvent, len(m.events))
	copy(result, m.events)
	return result
}

func TestNewEventBridge(t *testing.T) {
	subscriber, _ := chain.NewEventSubscriber(chain.EventSubscriberConfig{
		URL:          "ws://localhost:26657/websocket",
		ProviderUUID: "01234567-89ab-cdef-0123-456789abcdef",
	})
	publisher := &mockEventPublisher{}

	bridge := NewEventBridge(subscriber, publisher)

	if bridge == nil {
		t.Fatal("NewEventBridge() returned nil")
	}
	if bridge.subscriber != subscriber {
		t.Error("bridge.subscriber not set correctly")
	}
	if bridge.publisher != publisher {
		t.Error("bridge.publisher not set correctly")
	}
}

func TestEventBridge_Start_ForwardsEvents(t *testing.T) {
	subscriber, _ := chain.NewEventSubscriber(chain.EventSubscriberConfig{
		URL:          "ws://localhost:26657/websocket",
		ProviderUUID: "01234567-89ab-cdef-0123-456789abcdef",
	})
	publisher := &mockEventPublisher{}
	bridge := NewEventBridge(subscriber, publisher)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start bridge in goroutine
	errCh := make(chan error, 1)
	go func() {
		errCh <- bridge.Start(ctx)
	}()

	// Wait for bridge to subscribe
	select {
	case <-bridge.Ready():
	case <-time.After(5 * time.Second):
		t.Fatal("bridge did not start")
	}

	// Cancel context
	cancel()

	// Bridge should exit
	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("Start() error = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Error("Start() did not exit after context cancellation")
	}
}

func TestEventBridge_Start_ChannelClosed(t *testing.T) {
	subscriber, _ := chain.NewEventSubscriber(chain.EventSubscriberConfig{
		URL:          "ws://localhost:26657/websocket",
		ProviderUUID: "01234567-89ab-cdef-0123-456789abcdef",
	})
	publisher := &mockEventPublisher{}
	bridge := NewEventBridge(subscriber, publisher)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start bridge in goroutine
	errCh := make(chan error, 1)
	go func() {
		errCh <- bridge.Start(ctx)
	}()

	// Wait for bridge to subscribe
	select {
	case <-bridge.Ready():
	case <-time.After(5 * time.Second):
		t.Fatal("bridge did not start")
	}

	// Close the subscriber (simulates shutdown)
	subscriber.Close()

	// Bridge should exit gracefully
	select {
	case err := <-errCh:
		// Should return nil when channel is closed
		if err != nil {
			t.Errorf("Start() error = %v, want nil when channel closed", err)
		}
	case <-time.After(2 * time.Second):
		t.Error("Start() did not exit after subscriber closed")
	}
}

func TestEventBridge_Start_PublishError(t *testing.T) {
	// This test verifies that publish errors are logged but don't stop the bridge
	subscriber, _ := chain.NewEventSubscriber(chain.EventSubscriberConfig{
		URL:          "ws://localhost:26657/websocket",
		ProviderUUID: "01234567-89ab-cdef-0123-456789abcdef",
	})
	publisher := &mockEventPublisher{
		publishErr: errors.New("publish failed"),
	}
	bridge := NewEventBridge(subscriber, publisher)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start bridge in goroutine
	errCh := make(chan error, 1)
	go func() {
		errCh <- bridge.Start(ctx)
	}()

	// Wait for bridge to subscribe
	select {
	case <-bridge.Ready():
	case <-time.After(5 * time.Second):
		t.Fatal("bridge did not start")
	}

	// Cancel context to stop
	cancel()

	// Bridge should exit (publish errors shouldn't stop it)
	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("Start() error = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Error("Start() did not exit after context cancellation")
	}
}
