// Package testutil provides test fixtures and utilities for testing.
package testutil

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"github.com/cosmos/cosmos-sdk/crypto/keys/secp256k1"
	sdktypes "github.com/cosmos/cosmos-sdk/types"

	"github.com/manifest-network/fred/internal/adr036"
	"github.com/manifest-network/fred/internal/auth"
)

// TestKeyPair represents a key pair for testing.
type TestKeyPair struct {
	PrivKey   *secp256k1.PrivKey
	PubKey    *secp256k1.PubKey
	PubKeyB64 string
	Address   string // Bech32 address with "manifest" prefix
}

// NewTestKeyPair creates a deterministic test key pair from a seed.
func NewTestKeyPair(seed string) *TestKeyPair {
	// Create deterministic private key from seed
	hash := sha256.Sum256([]byte(seed))
	privKey := &secp256k1.PrivKey{Key: hash[:]}
	pubKey := privKey.PubKey().(*secp256k1.PubKey)

	// Use SDK's bech32 encoding
	addr, err := sdktypes.Bech32ifyAddressBytes("manifest", pubKey.Address().Bytes())
	if err != nil {
		panic(fmt.Sprintf("failed to encode address: %v", err))
	}

	return &TestKeyPair{
		PrivKey:   privKey,
		PubKey:    pubKey,
		PubKeyB64: base64.StdEncoding.EncodeToString(pubKey.Bytes()),
		Address:   addr,
	}
}

// Sign signs a message using ADR-036 format and returns base64-encoded signature.
func (kp *TestKeyPair) Sign(message []byte) string {
	signBytes := adr036.CreateSignBytes(message, kp.Address)
	sig, err := kp.PrivKey.Sign(signBytes)
	if err != nil {
		panic(fmt.Sprintf("failed to sign: %v", err))
	}
	return base64.StdEncoding.EncodeToString(sig)
}

// TestAuthToken creates a test authentication token.
type TestAuthToken struct {
	Tenant    string `json:"tenant"`
	LeaseUUID string `json:"lease_uuid"`
	Timestamp int64  `json:"timestamp"`
	PubKey    string `json:"pub_key"`
	Signature string `json:"signature"`
}

// CreateTestToken creates a valid auth token for testing.
func CreateTestToken(kp *TestKeyPair, leaseUUID string, timestamp time.Time) string {
	token := TestAuthToken{
		Tenant:    kp.Address,
		LeaseUUID: leaseUUID,
		Timestamp: timestamp.Unix(),
		PubKey:    kp.PubKeyB64,
	}

	// Create sign data using the canonical format
	signData := auth.FormatSignData(token.Tenant, token.LeaseUUID, token.Timestamp)
	token.Signature = kp.Sign(signData)

	// Encode to base64
	jsonBytes, err := json.Marshal(token)
	if err != nil {
		panic(fmt.Sprintf("failed to marshal token: %v", err))
	}
	return base64.StdEncoding.EncodeToString(jsonBytes)
}

// CreateExpiredToken creates an expired auth token for testing.
func CreateExpiredToken(kp *TestKeyPair, leaseUUID string) string {
	return CreateTestToken(kp, leaseUUID, time.Now().Add(-10*time.Minute))
}

// CreateFutureToken creates a future-dated auth token for testing.
func CreateFutureToken(kp *TestKeyPair, leaseUUID string) string {
	return CreateTestToken(kp, leaseUUID, time.Now().Add(10*time.Minute))
}

// Sample UUIDs for testing
const (
	ValidUUID1   = "01234567-89ab-cdef-0123-456789abcdef"
	ValidUUID2   = "abcdef01-2345-6789-abcd-ef0123456789"
	ValidUUID3   = "12345678-1234-1234-1234-123456789abc"
	InvalidUUID1 = "not-a-uuid"
	InvalidUUID2 = "01234567-89ab-cdef-0123"
	InvalidUUID3 = "01234567-89ab-cdef-0123-456789abcdefg"
)

// TestPayloadAuthToken creates a test payload authentication token.
type TestPayloadAuthToken struct {
	Tenant    string `json:"tenant"`
	LeaseUUID string `json:"lease_uuid"`
	MetaHash  string `json:"meta_hash"`
	Timestamp int64  `json:"timestamp"`
	PubKey    string `json:"pub_key"`
	Signature string `json:"signature"`
}

// CreateTestPayloadToken creates a valid payload auth token for testing.
func CreateTestPayloadToken(kp *TestKeyPair, leaseUUID, metaHashHex string, timestamp time.Time) string {
	token := TestPayloadAuthToken{
		Tenant:    kp.Address,
		LeaseUUID: leaseUUID,
		MetaHash:  metaHashHex,
		Timestamp: timestamp.Unix(),
		PubKey:    kp.PubKeyB64,
	}

	// Create sign data using the payload-specific format
	signData := auth.FormatPayloadSignData(token.LeaseUUID, token.MetaHash, token.Timestamp)
	token.Signature = kp.Sign(signData)

	// Encode to base64
	jsonBytes, err := json.Marshal(token)
	if err != nil {
		panic(fmt.Sprintf("failed to marshal token: %v", err))
	}
	return base64.StdEncoding.EncodeToString(jsonBytes)
}

// CreateExpiredPayloadToken creates an expired payload auth token for testing.
func CreateExpiredPayloadToken(kp *TestKeyPair, leaseUUID, metaHashHex string) string {
	return CreateTestPayloadToken(kp, leaseUUID, metaHashHex, time.Now().Add(-10*time.Minute))
}

// CreateFuturePayloadToken creates a future-dated payload auth token for testing.
func CreateFuturePayloadToken(kp *TestKeyPair, leaseUUID, metaHashHex string) string {
	return CreateTestPayloadToken(kp, leaseUUID, metaHashHex, time.Now().Add(10*time.Minute))
}

// ComputePayloadHash computes the SHA-256 hash of a payload and returns it as hex.
func ComputePayloadHash(payload []byte) string {
	hash := sha256.Sum256(payload)
	return hex.EncodeToString(hash[:])
}
