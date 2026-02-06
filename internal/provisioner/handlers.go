package provisioner

import (
	"encoding/json"
	"log/slog"

	"github.com/ThreeDotsLabs/watermill/message"

	"github.com/manifest-network/fred/internal/metrics"
)

// recordWatermillMetrics records the outcome of a Watermill message handler.
func recordWatermillMetrics(topic string, err error) {
	outcome := metrics.OutcomeSuccess
	if err != nil {
		outcome = metrics.OutcomeError
	}
	metrics.WatermillMessagesTotal.WithLabelValues(topic, outcome).Inc()
}

// unmarshalMessagePayload unmarshals a Watermill message payload into the given type.
// On failure it records a malformed message metric, logs the error, and returns false.
// Callers should return nil (skip retry) when ok is false.
func unmarshalMessagePayload[T any](msg *message.Message, topic string) (T, bool) {
	var v T
	if err := json.Unmarshal(msg.Payload, &v); err != nil {
		metrics.MalformedMessagesTotal.WithLabelValues(topic).Inc()
		slog.Error("failed to unmarshal message",
			"topic", topic,
			"error", err,
			"error_type", ErrMalformedMessage,
		)
		return v, false
	}
	return v, true
}

// The following methods delegate to the HandlerSet for backwards compatibility
// with existing tests and code that calls handlers via the Manager.

// handleLeaseCreated delegates to the handler set.
func (m *Manager) handleLeaseCreated(msg *message.Message) error {
	return m.handlers.HandleLeaseCreated(msg)
}

// handleLeaseClosed delegates to the handler set.
func (m *Manager) handleLeaseClosed(msg *message.Message) error {
	return m.handlers.HandleLeaseClosed(msg)
}

// handleLeaseExpired delegates to the handler set.
func (m *Manager) handleLeaseExpired(msg *message.Message) error {
	return m.handlers.HandleLeaseExpired(msg)
}

// handleBackendCallback delegates to the handler set.
func (m *Manager) handleBackendCallback(msg *message.Message) error {
	return m.handlers.HandleBackendCallback(msg)
}

// handlePayloadReceived delegates to the handler set.
func (m *Manager) handlePayloadReceived(msg *message.Message) error {
	return m.handlers.HandlePayloadReceived(msg)
}
