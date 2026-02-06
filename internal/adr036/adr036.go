// Package adr036 implements ADR-036 off-chain message signing and verification.
// See: https://docs.cosmos.network/main/build/architecture/adr-036-arbitrary-signature
//
// Note: This package uses custom Coin/Fee types instead of sdk.Coin because ADR-036
// requires the legacy Amino JSON format with string amounts. The SDK's sdk.Coin uses
// math.Int which serializes differently and would produce invalid sign documents that
// wallets cannot verify.
package adr036

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/big"

	"github.com/cosmos/cosmos-sdk/crypto/keys/secp256k1"
)

// secp256k1 curve order N and half-order N/2 for low-S normalization.
// These are the standard values for the secp256k1 curve used by Bitcoin and Cosmos.
var (
	secp256k1N, _        = new(big.Int).SetString("FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFEBAAEDCE6AF48A03BBFD25E8CD0364141", 16)
	secp256k1HalfN       = new(big.Int).Rsh(secp256k1N, 1)
)

// SignDoc represents the ADR-036 sign document structure.
// This is the canonical format that wallets sign for off-chain messages.
type SignDoc struct {
	AccountNumber string       `json:"account_number"`
	ChainID       string       `json:"chain_id"`
	Fee           Fee          `json:"fee"`
	Memo          string       `json:"memo"`
	Msgs          []MsgWrapper `json:"msgs"`
	Sequence      string       `json:"sequence"`
}

// Coin represents a coin amount (used in fee structure).
type Coin struct {
	Denom  string `json:"denom"`
	Amount string `json:"amount"`
}

// Fee represents the fee structure in ADR-036 (always zero for off-chain).
type Fee struct {
	Amount []Coin `json:"amount"`
	Gas    string `json:"gas"`
}

// MsgWrapper wraps the ADR-036 message.
type MsgWrapper struct {
	Type  string   `json:"type"`
	Value MsgValue `json:"value"`
}

// MsgValue contains the actual sign data.
type MsgValue struct {
	Data   string `json:"data"`   // Base64-encoded message data
	Signer string `json:"signer"` // Bech32 address of the signer
}

// NormalizeToLowS normalizes a 64-byte secp256k1 signature to low-S canonical form.
// ECDSA signatures have two valid forms: (r, s) and (r, N-s). This function
// ensures the S value is in the lower half of the curve order, producing a
// unique canonical representation. This prevents signature malleability where
// an attacker flips the S value to create a different-but-valid signature.
//
// Returns the normalized signature (may be the same slice if already low-S),
// or nil if the input is not a valid 64-byte signature.
func NormalizeToLowS(sig []byte) []byte {
	if len(sig) != 64 {
		return nil
	}

	// Extract S from the second 32 bytes
	s := new(big.Int).SetBytes(sig[32:64])

	// If S > N/2, replace with N - S
	if s.Cmp(secp256k1HalfN) > 0 {
		s.Sub(secp256k1N, s)
		normalized := make([]byte, 64)
		copy(normalized[:32], sig[:32]) // R unchanged
		sBytes := s.Bytes()
		// Left-pad S to 32 bytes
		copy(normalized[64-len(sBytes):64], sBytes)
		return normalized
	}

	return sig
}

// VerifySignature verifies an ADR-036 off-chain signature.
// The signature is normalized to low-S canonical form before verification
// to accept both (r, s) and (r, N-s) forms.
//
// Parameters:
//   - pubKeyBytes: 33-byte compressed secp256k1 public key
//   - message: the original message that was signed
//   - signature: the signature bytes (64 bytes for secp256k1)
//   - signer: the bech32 address of the signer (used in the sign doc)
//
// Returns nil if the signature is valid, otherwise an error.
func VerifySignature(pubKeyBytes, message, signature []byte, signer string) error {
	if len(pubKeyBytes) != 33 {
		return fmt.Errorf("invalid public key length: expected 33, got %d", len(pubKeyBytes))
	}

	// Create the sign document bytes
	signBytes := CreateSignBytes(message, signer)
	if signBytes == nil {
		return fmt.Errorf("failed to create sign bytes")
	}

	// Normalize signature to low-S form before verification.
	// This ensures both (r, s) and (r, N-s) forms are accepted.
	normalized := NormalizeToLowS(signature)
	if normalized == nil {
		return fmt.Errorf("invalid signature length: expected 64, got %d", len(signature))
	}

	// Create secp256k1 public key and verify
	pubKey := &secp256k1.PubKey{Key: pubKeyBytes}
	if !pubKey.VerifySignature(signBytes, normalized) {
		return fmt.Errorf("signature verification failed")
	}

	return nil
}

// CreateSignBytes creates the bytes to be signed for ADR-036.
// This is the canonical JSON serialization of the SignDoc.
func CreateSignBytes(message []byte, signer string) []byte {
	signDoc := SignDoc{
		AccountNumber: "0",
		ChainID:       "",
		Fee: Fee{
			Amount: []Coin{},
			Gas:    "0",
		},
		Memo: "",
		Msgs: []MsgWrapper{
			{
				Type: "sign/MsgSignData",
				Value: MsgValue{
					Data:   base64.StdEncoding.EncodeToString(message),
					Signer: signer,
				},
			},
		},
		Sequence: "0",
	}

	// JSON serialize - Go's json.Marshal produces sorted keys for structs
	signBytes, err := json.Marshal(signDoc)
	if err != nil {
		return nil
	}

	return signBytes
}

// CreateSignDoc creates an ADR-036 SignDoc for the given message and signer.
// This can be used by clients to construct the document they need to sign.
func CreateSignDoc(message []byte, signer string) *SignDoc {
	return &SignDoc{
		AccountNumber: "0",
		ChainID:       "",
		Fee: Fee{
			Amount: []Coin{},
			Gas:    "0",
		},
		Memo: "",
		Msgs: []MsgWrapper{
			{
				Type: "sign/MsgSignData",
				Value: MsgValue{
					Data:   base64.StdEncoding.EncodeToString(message),
					Signer: signer,
				},
			},
		},
		Sequence: "0",
	}
}
