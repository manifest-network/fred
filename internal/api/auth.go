package api

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"

	"github.com/manifest-network/fred/internal/auth"
)

const (
	// MaxTokenAge is the maximum age of a valid authentication token.
	// Kept short (30 seconds) to limit replay attack window.
	MaxTokenAge = 30 * time.Second
)

// AuthToken represents the bearer token for tenant authentication.
type AuthToken struct {
	Tenant    string `json:"tenant"`
	LeaseUUID string `json:"lease_uuid"`
	Timestamp int64  `json:"timestamp"`
	PubKey    string `json:"pub_key"`   // Base64-encoded public key
	Signature string `json:"signature"` // Base64-encoded signature
}

// ParseAuthToken parses a base64-encoded authentication token.
func ParseAuthToken(encoded string) (*AuthToken, error) {
	decoded, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return nil, fmt.Errorf("failed to decode token: %w", err)
	}

	var token AuthToken
	if err := json.Unmarshal(decoded, &token); err != nil {
		return nil, fmt.Errorf("failed to unmarshal token: %w", err)
	}

	return &token, nil
}

// Validate verifies the token's timestamp and ADR-036 signature.
// The bech32Prefix is used to verify the tenant address matches the public key.
func (t *AuthToken) Validate(bech32Prefix string) error {
	// Validate required fields
	if t.LeaseUUID == "" {
		return fmt.Errorf("lease_uuid is required")
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

// createSignData creates the message data to be signed.
func (t *AuthToken) createSignData() []byte {
	return auth.FormatSignData(t.Tenant, t.LeaseUUID, t.Timestamp)
}
