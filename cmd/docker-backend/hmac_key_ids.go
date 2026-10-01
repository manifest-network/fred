package main

import (
	"encoding/json"
	"io"

	"github.com/manifest-network/fred/internal/backend/docker"
)

// requestKeyIDs is the -print-hmac-key-ids output: non-secret IDs that let an
// operator confirm, before a rotation step, that providerd holds matching
// keys. An ID still lets someone test guesses of a weak key, so IDs are printed
// only on request and never logged.
type requestKeyIDs struct {
	CurrentKeyID string `json:"current_key_id"`
	NextKeyID    string `json:"next_key_id,omitempty"`
}

func printRequestKeyIDs(w io.Writer, cfg docker.Config) error {
	keys, err := cfg.RequestKeys()
	if err != nil {
		return err
	}
	ids := requestKeyIDs{CurrentKeyID: keys.CurrentKeyID()}
	if next, rotating := keys.RotationKeyID(); rotating {
		ids.NextKeyID = next
	}
	return json.NewEncoder(w).Encode(ids)
}
