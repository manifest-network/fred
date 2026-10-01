package main

import (
	"encoding/json"
	"io"

	"github.com/manifest-network/fred/internal/config"
)

// backendKeyIDs is one backend's entry in the --print-hmac-key-ids output.
// Key IDs are non-secret fingerprints that let an operator confirm, before a
// rotation step, that providerd and a backend hold matching keys. An ID still
// lets someone test guesses of a weak key, so IDs are printed only on request
// and never logged.
type backendKeyIDs struct {
	Name          string `json:"name"`
	CurrentKeyID  string `json:"current_key_id"`
	PreviousKeyID string `json:"previous_key_id,omitempty"`
}

func printBackendKeyIDs(w io.Writer, cfg *config.Config) error {
	output := struct {
		Backends []backendKeyIDs `json:"backends"`
	}{Backends: make([]backendKeyIDs, 0, len(cfg.Backends))}
	for _, configuredBackend := range cfg.Backends {
		keys, err := cfg.BackendCallbackKeys(configuredBackend.Name)
		if err != nil {
			return err
		}
		entry := backendKeyIDs{Name: configuredBackend.Name, CurrentKeyID: keys.CurrentKeyID()}
		if previous, rotating := keys.RotationKeyID(); rotating {
			entry.PreviousKeyID = previous
		}
		output.Backends = append(output.Backends, entry)
	}
	return json.NewEncoder(w).Encode(output)
}
