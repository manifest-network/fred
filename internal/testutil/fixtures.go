// Package testutil provides test fixtures and utilities for testing.
package testutil

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"time"

	"github.com/cosmos/cosmos-sdk/crypto/keys/secp256k1"
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

	return &TestKeyPair{
		PrivKey:   privKey,
		PubKey:    pubKey,
		PubKeyB64: base64.StdEncoding.EncodeToString(pubKey.Bytes()),
		Address:   pubKeyToAddress(pubKey, "manifest"),
	}
}

// Sign signs a message using ADR-036 format and returns base64-encoded signature.
func (kp *TestKeyPair) Sign(message []byte) string {
	signBytes := CreateADR036SignBytes(message, kp.Address)
	sig, _ := kp.PrivKey.Sign(signBytes)
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

	// Create sign data
	signData := []byte(token.Tenant + ":" + token.LeaseUUID + ":" + itoa(token.Timestamp))
	token.Signature = kp.Sign(signData)

	// Encode to base64
	jsonBytes, _ := json.Marshal(token)
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

// CreateADR036SignBytes creates the bytes to be signed for ADR-036.
func CreateADR036SignBytes(message []byte, signer string) []byte {
	signDoc := map[string]interface{}{
		"account_number": "0",
		"chain_id":       "",
		"fee": map[string]interface{}{
			"amount": []interface{}{},
			"gas":    "0",
		},
		"memo": "",
		"msgs": []map[string]interface{}{
			{
				"type": "sign/MsgSignData",
				"value": map[string]interface{}{
					"data":   base64.StdEncoding.EncodeToString(message),
					"signer": signer,
				},
			},
		},
		"sequence": "0",
	}

	// JSON serialize with sorted keys
	signBytes, _ := json.Marshal(signDoc)
	return signBytes
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

// pubKeyToAddress converts a public key to a bech32 address.
func pubKeyToAddress(pubKey *secp256k1.PubKey, prefix string) string {
	addr := pubKey.Address()
	return bech32Encode(prefix, addr.Bytes())
}

// bech32Encode encodes data to bech32 format.
func bech32Encode(prefix string, data []byte) string {
	converted, _ := convertBits(data, 8, 5, true)
	checksum := createChecksum(prefix, converted)
	combined := append(converted, checksum...)

	charset := "qpzry9x8gf2tvdw0s3jn54khce6mua7l"
	result := prefix + "1"
	for _, b := range combined {
		result += string(charset[b])
	}
	return result
}

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

	if pad && bits > 0 {
		ret = append(ret, byte((acc<<(toBits-bits))&maxv))
	}

	return ret, nil
}

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

func hrpExpand(hrp string) []byte {
	ret := make([]byte, len(hrp)*2+1)
	for i, c := range hrp {
		ret[i] = byte(c >> 5)
		ret[i+len(hrp)+1] = byte(c & 31)
	}
	ret[len(hrp)] = 0
	return ret
}

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

func itoa(n int64) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	neg := n < 0
	if neg {
		n = -n
	}
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
