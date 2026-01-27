package api

import (
	"encoding/hex"
	"fmt"

	"github.com/manifest-network/fred/internal/auth"
)

// PayloadAuthToken represents the bearer token for payload upload authentication.
// It includes meta_hash to bind the signature to a specific payload.
type PayloadAuthToken struct {
	Tenant    string `json:"tenant"`
	LeaseUUID string `json:"lease_uuid"`
	MetaHash  string `json:"meta_hash"` // Hex-encoded SHA-256 hash
	Timestamp int64  `json:"timestamp"`
	PubKey    string `json:"pub_key"`   // Base64-encoded public key
	Signature string `json:"signature"` // Base64-encoded signature
}

// ParsePayloadAuthToken parses a base64-encoded payload authentication token.
func ParsePayloadAuthToken(encoded string) (*PayloadAuthToken, error) {
	return parseBase64Token[PayloadAuthToken](encoded)
}

// Validate verifies the token's timestamp and ADR-036 signature.
// The bech32Prefix is used to verify the tenant address matches the public key.
func (t *PayloadAuthToken) Validate(bech32Prefix string) error {
	// Validate required fields specific to payload tokens
	if t.LeaseUUID == "" {
		return fmt.Errorf("lease_uuid is required")
	}
	if t.MetaHash == "" {
		return fmt.Errorf("meta_hash is required")
	}
	// Validate meta_hash format (SHA-256 = 32 bytes = 64 hex chars)
	if len(t.MetaHash) != 64 {
		return fmt.Errorf("meta_hash must be 64 hex characters (SHA-256), got %d", len(t.MetaHash))
	}
	if _, err := hex.DecodeString(t.MetaHash); err != nil {
		return fmt.Errorf("meta_hash must be valid hex: %w", err)
	}

	// Use common validation logic
	v := &tokenValidator{
		tenant:    t.Tenant,
		timestamp: t.Timestamp,
		pubKey:    t.PubKey,
		signature: t.Signature,
	}

	return v.validateCommon(t.createSignData(), bech32Prefix)
}

// createSignData creates the message data to be signed for payload upload.
// Format: "manifest lease data {lease_uuid} {meta_hash_hex} {unix_timestamp}"
func (t *PayloadAuthToken) createSignData() []byte {
	return auth.FormatPayloadSignData(t.LeaseUUID, t.MetaHash, t.Timestamp)
}

