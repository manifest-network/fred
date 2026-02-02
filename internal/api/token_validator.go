package api

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"

	"github.com/cosmos/cosmos-sdk/crypto/keys/secp256k1"
	sdktypes "github.com/cosmos/cosmos-sdk/types"

	"github.com/manifest-network/fred/internal/adr036"
)

// parseBase64Token decodes a base64-encoded JSON token into the specified type.
// This generic helper consolidates the common parsing logic used by all token types.
func parseBase64Token[T any](encoded string) (*T, error) {
	decoded, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return nil, fmt.Errorf("failed to decode token: %w", err)
	}

	var token T
	if err := json.Unmarshal(decoded, &token); err != nil {
		return nil, fmt.Errorf("failed to unmarshal token: %w", err)
	}

	return &token, nil
}

// tokenValidator provides common validation logic for authentication tokens.
// This is used by both AuthToken and PayloadAuthToken to avoid code duplication.
type tokenValidator struct {
	tenant    string
	timestamp int64
	pubKey    string
	signature string
}

// validateCommon performs common token validation: timestamp, signature, and address verification.
// The signData parameter is the message that was signed (differs by token type).
func (v *tokenValidator) validateCommon(signData []byte, bech32Prefix string) error {
	// Validate tenant is present
	if v.tenant == "" {
		return fmt.Errorf("tenant is required")
	}

	// Check timestamp
	tokenTime := time.Unix(v.timestamp, 0)
	if time.Since(tokenTime) > MaxTokenAge {
		return fmt.Errorf("token expired: issued at %v", tokenTime)
	}
	// Use tighter tolerance for future timestamps to limit pre-generated token attacks
	if time.Until(tokenTime) > MaxFutureClockSkew {
		return fmt.Errorf("token timestamp too far in future: %v (max skew: %v)", tokenTime, MaxFutureClockSkew)
	}

	// Decode public key
	pubKeyBytes, err := base64.StdEncoding.DecodeString(v.pubKey)
	if err != nil {
		return fmt.Errorf("failed to decode public key: %w", err)
	}

	// Decode signature
	sigBytes, err := base64.StdEncoding.DecodeString(v.signature)
	if err != nil {
		return fmt.Errorf("failed to decode signature: %w", err)
	}

	// Verify ADR-036 signature
	if err := adr036.VerifySignature(pubKeyBytes, signData, sigBytes, v.tenant); err != nil {
		return fmt.Errorf("signature verification failed: %w", err)
	}

	// Verify the public key corresponds to the tenant address
	if err := v.verifyAddress(pubKeyBytes, bech32Prefix); err != nil {
		return fmt.Errorf("address verification failed: %w", err)
	}

	return nil
}

// verifyAddress verifies that the public key corresponds to the tenant address.
func (v *tokenValidator) verifyAddress(pubKeyBytes []byte, bech32Prefix string) error {
	if len(pubKeyBytes) != 33 {
		return fmt.Errorf("invalid public key length: got %d, want 33", len(pubKeyBytes))
	}

	pubKey := &secp256k1.PubKey{Key: pubKeyBytes}
	derivedAddr, err := sdktypes.Bech32ifyAddressBytes(bech32Prefix, pubKey.Address().Bytes())
	if err != nil {
		return fmt.Errorf("failed to convert address to bech32: %w", err)
	}

	if derivedAddr != v.tenant {
		return fmt.Errorf("public key does not match tenant address: got %s, want %s", derivedAddr, v.tenant)
	}

	return nil
}
