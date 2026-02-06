package api

import (
	"fmt"
	"time"

	"github.com/manifest-network/fred/internal/auth"
)

const (
	// MaxTokenAge is the maximum age of a valid authentication token.
	// Kept short (30 seconds) to limit replay attack window.
	MaxTokenAge = 30 * time.Second

	// MaxFutureClockSkew is the maximum allowed clock skew for tokens with future timestamps.
	// This is intentionally smaller than MaxTokenAge to limit pre-generated token attacks.
	// 10 seconds allows for reasonable clock drift without enabling abuse.
	MaxFutureClockSkew = 10 * time.Second
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
	return parseBase64Token[AuthToken](encoded)
}

// Validate verifies the token's timestamp and ADR-036 signature.
// The bech32Prefix is used to verify the tenant address matches the public key.
// On success, t.Signature is updated to the low-S canonical form to ensure
// consistent replay tracking regardless of which signature variant was submitted.
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

	if err := v.validateCommon(t.createSignData(), bech32Prefix); err != nil {
		return err
	}

	// Copy back the normalized signature for consistent replay tracking
	t.Signature = v.signature
	return nil
}

// createSignData creates the message data to be signed.
func (t *AuthToken) createSignData() []byte {
	return auth.FormatSignData(t.Tenant, t.LeaseUUID, t.Timestamp)
}
