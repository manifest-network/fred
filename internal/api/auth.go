package api

import (
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"github.com/cosmos/cosmos-sdk/crypto/keys/secp256k1"

	"github.com/manifest-network/fred/internal/adr036"
)

const (
	// MaxTokenAge is the maximum age of a valid authentication token.
	MaxTokenAge = 5 * time.Minute
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
	// The data to sign: tenant + lease_uuid + timestamp
	data := fmt.Sprintf("%s:%s:%d", t.Tenant, t.LeaseUUID, t.Timestamp)
	return []byte(data)
}

// verifyAddress verifies that the public key corresponds to the tenant address.
func (t *AuthToken) verifyAddress(pubKeyBytes []byte, bech32Prefix string) error {
	if len(pubKeyBytes) != 33 {
		return fmt.Errorf("invalid public key length")
	}

	pubKey := &secp256k1.PubKey{Key: pubKeyBytes}
	addr := pubKey.Address()

	// Convert to bech32 address with configured prefix
	expectedAddr := addressToBech32(bech32Prefix, addr.Bytes())

	if expectedAddr != t.Tenant {
		return fmt.Errorf("public key does not match tenant address: expected %s, got %s", t.Tenant, expectedAddr)
	}

	return nil
}

// addressToBech32 converts raw address bytes to a bech32 address.
func addressToBech32(prefix string, addrBytes []byte) string {
	return bech32Encode(prefix, addrBytes)
}

// bech32Encode encodes data to bech32 format.
func bech32Encode(prefix string, data []byte) string {
	converted, err := convertBits(data, 8, 5, true)
	if err != nil {
		return ""
	}

	checksum := createChecksum(prefix, converted)
	combined := append(converted, checksum...)

	result := prefix + "1"
	for _, b := range combined {
		result += string(charset[b])
	}

	return result
}

const charset = "qpzry9x8gf2tvdw0s3jn54khce6mua7l"

// convertBits converts between bit groups.
func convertBits(data []byte, fromBits, toBits uint, pad bool) ([]byte, error) {
	acc := 0
	bits := uint(0)
	var ret []byte
	maxv := (1 << toBits) - 1

	for _, value := range data {
		acc = (acc << fromBits) | int(value)
		bits += fromBits
		for bits >= toBits {
			bits -= toBits
			ret = append(ret, byte((acc>>bits)&maxv))
		}
	}

	if pad {
		if bits > 0 {
			ret = append(ret, byte((acc<<(toBits-bits))&maxv))
		}
	} else if bits >= fromBits || ((acc<<(toBits-bits))&maxv) != 0 {
		return nil, fmt.Errorf("invalid padding")
	}

	return ret, nil
}

// createChecksum creates a bech32 checksum.
func createChecksum(prefix string, data []byte) []byte {
	values := append(hrpExpand(prefix), data...)
	values = append(values, []byte{0, 0, 0, 0, 0, 0}...)
	polymod := bech32Polymod(values) ^ 1
	checksum := make([]byte, 6)
	for i := 0; i < 6; i++ {
		checksum[i] = byte((polymod >> (5 * (5 - i))) & 31)
	}
	return checksum
}

// hrpExpand expands the human-readable part for checksum calculation.
func hrpExpand(hrp string) []byte {
	ret := make([]byte, len(hrp)*2+1)
	for i, c := range hrp {
		ret[i] = byte(c >> 5)
		ret[i+len(hrp)+1] = byte(c & 31)
	}
	ret[len(hrp)] = 0
	return ret
}

// bech32Polymod calculates the BCH checksum.
func bech32Polymod(values []byte) int {
	generator := []int{0x3b6a57b2, 0x26508e6d, 0x1ea119fa, 0x3d4233dd, 0x2a1462b3}
	chk := 1
	for _, v := range values {
		top := chk >> 25
		chk = (chk&0x1ffffff)<<5 ^ int(v)
		for i := 0; i < 5; i++ {
			if (top>>i)&1 == 1 {
				chk ^= generator[i]
			}
		}
	}
	return chk
}

// HashToHex converts raw bytes to hex string.
func HashToHex(data []byte) string {
	return hex.EncodeToString(data)
}
