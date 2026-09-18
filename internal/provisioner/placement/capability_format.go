package placement

import (
	"fmt"
	"log/slog"
)

var (
	_ fmt.Formatter  = CallbackPair{}
	_ slog.LogValuer = CallbackPair{}
	_ fmt.Formatter  = AttemptMetadata{}
	_ slog.LogValuer = AttemptMetadata{}
	_ fmt.Formatter  = MaintenanceCommand{}
	_ slog.LogValuer = MaintenanceCommand{}
)

const (
	callbackPairDiagnostic       = "placement.CallbackPair{redacted}"
	attemptMetadataDiagnostic    = "placement.AttemptMetadata{redacted}"
	maintenanceCommandDiagnostic = "placement.MaintenanceCommand{redacted}"
)

// Format prevents reflective formatting from exposing either persisted
// callback URL or its raw operation generation.
func (pair CallbackPair) Format(state fmt.State, _ rune) {
	_, _ = state.Write([]byte(callbackPairDiagnostic))
}

// LogValue makes direct structured logging of a callback pair safe.
func (pair CallbackPair) LogValue() slog.Value {
	return slog.StringValue(callbackPairDiagnostic)
}

// Format prevents reflective formatting from exposing the complete durable
// retry authority, including callback URLs, request identity, and restore
// source.
func (metadata AttemptMetadata) Format(state fmt.State, _ rune) {
	_, _ = state.Write([]byte(attemptMetadataDiagnostic))
}

// LogValue makes direct structured logging of attempt metadata safe.
func (metadata AttemptMetadata) LogValue() slog.Value {
	return slog.StringValue(attemptMetadataDiagnostic)
}

// Format prevents reflective formatting from exposing a maintenance payload,
// callback URL, lifecycle generation, or storage identity.
func (command MaintenanceCommand) Format(state fmt.State, _ rune) {
	_, _ = state.Write([]byte(maintenanceCommandDiagnostic))
}

// LogValue makes direct structured logging of a maintenance command safe.
func (command MaintenanceCommand) LogValue() slog.Value {
	return slog.StringValue(maintenanceCommandDiagnostic)
}
