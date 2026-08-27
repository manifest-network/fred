package provisioner

import (
	"encoding/json"
	"log/slog"

	"github.com/ThreeDotsLabs/watermill/message"

	"github.com/manifest-network/fred/internal/metrics"
)

// recordWatermillMetrics records the outcome under the established internal
// message-topic metric labels. Callback ingress uses the same label even though
// Manager invokes that adapter synchronously rather than through Watermill.
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
