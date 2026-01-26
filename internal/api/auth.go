package api

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"

	"github.com/cosmos/cosmos-sdk/crypto/keys/secp256k1"
	sdktypes "github.com/cosmos/cosmos-sdk/types"

	"github.com/manifest-network/fred/internal/adr036"
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
	// Check timestamp
	tokenTime := time.Unix(t.Timestamp, 0)
	if time.Since(tokenTime) > MaxTokenAge {
		return fmt.Errorf("token expired: issued at %v", tokenTime)
	}
	if time.Until(tokenTime) > MaxTokenAge {
		return fmt.Errorf("token timestamp is in the future: %v", tokenTime)
	}

	// Decode public key
	pubKeyBytes, err := base64.StdEncoding.DecodeString(t.PubKey)
	if err != nil {
		return fmt.Errorf("failed to decode public key: %w", err)
	}

	// Decode signature
	sigBytes, err := base64.StdEncoding.DecodeString(t.Signature)
	if err != nil {
		return fmt.Errorf("failed to decode signature: %w", err)
	}

	// Create the sign data (message to be signed)
	signData := t.createSignData()

	// Verify ADR-036 signature
	if err := adr036.VerifySignature(pubKeyBytes, signData, sigBytes, t.Tenant); err != nil {
		return fmt.Errorf("signature verification failed: %w", err)
	}

	// Verify the public key corresponds to the tenant address
	if err := t.verifyAddress(pubKeyBytes, bech32Prefix); err != nil {
		return fmt.Errorf("address verification failed: %w", err)
	}

	return nil
}

// createSignData creates the message data to be signed.
func (t *AuthToken) createSignData() []byte {
	return auth.FormatSignData(t.Tenant, t.LeaseUUID, t.Timestamp)
}

// verifyAddress verifies that the public key corresponds to the tenant address.
func (t *AuthToken) verifyAddress(pubKeyBytes []byte, bech32Prefix string) error {
	if len(pubKeyBytes) != 33 {
		return fmt.Errorf("invalid public key length")
	}

	pubKey := &secp256k1.PubKey{Key: pubKeyBytes}
	addr := pubKey.Address()

	// Convert to bech32 address with configured prefix
	expectedAddr, err := sdktypes.Bech32ifyAddressBytes(bech32Prefix, addr.Bytes())
	if err != nil {
		return fmt.Errorf("failed to convert address to bech32: %w", err)
	}

	if expectedAddr != t.Tenant {
		return fmt.Errorf("public key does not match tenant address: expected %s, got %s", t.Tenant, expectedAddr)
	}

	return nil
}
