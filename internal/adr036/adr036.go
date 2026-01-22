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

	"github.com/cosmos/cosmos-sdk/crypto/keys/secp256k1"
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

// VerifySignature verifies an ADR-036 off-chain signature.
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

	// Create secp256k1 public key and verify
	pubKey := &secp256k1.PubKey{Key: pubKeyBytes}
	if !pubKey.VerifySignature(signBytes, signature) {
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
