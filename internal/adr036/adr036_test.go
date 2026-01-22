package adr036_test

import (
	"encoding/base64"
	"encoding/json"
	"testing"

	"github.com/manifest-network/fred/internal/adr036"
	"github.com/manifest-network/fred/internal/testutil"
)

func TestVerifySignature_Valid(t *testing.T) {
	kp := testutil.NewTestKeyPair("test-seed-1")

	message := []byte("test message")
	signBytes := adr036.CreateSignBytes(message, kp.Address)
	sig, err := kp.PrivKey.Sign(signBytes)
	if err != nil {
		t.Fatalf("Sign() error = %v", err)
	}

	err = adr036.VerifySignature(kp.PubKey.Bytes(), message, sig, kp.Address)
	if err != nil {
		t.Errorf("VerifySignature() error = %v, want nil", err)
	}
}

func TestVerifySignature_InvalidPubKeyLength(t *testing.T) {
	tests := []struct {
		name      string
		pubKeyLen int
	}{
		{"too short", 32},
		{"too long", 34},
		{"empty", 0},
		{"very short", 10},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pubKey := make([]byte, tt.pubKeyLen)
			err := adr036.VerifySignature(pubKey, []byte("test"), []byte("sig"), "signer")
			if err == nil {
				t.Error("VerifySignature() = nil, want error")
			}
			if err != nil && tt.pubKeyLen != 33 {
				expected := "invalid public key length"
				if err.Error()[:len(expected)] != expected {
					t.Errorf("VerifySignature() error = %q, want to start with %q", err.Error(), expected)
				}
			}
		})
	}
}

func TestVerifySignature_InvalidSignature(t *testing.T) {
	kp := testutil.NewTestKeyPair("test-seed-2")

	message := []byte("test message")
	wrongSig := make([]byte, 64) // Invalid signature

	err := adr036.VerifySignature(kp.PubKey.Bytes(), message, wrongSig, kp.Address)
	if err == nil {
		t.Error("VerifySignature() = nil, want error")
	}
}

func TestVerifySignature_WrongMessage(t *testing.T) {
	kp := testutil.NewTestKeyPair("test-seed-3")

	originalMessage := []byte("original message")
	signBytes := adr036.CreateSignBytes(originalMessage, kp.Address)
	sig, err := kp.PrivKey.Sign(signBytes)
	if err != nil {
		t.Fatalf("Sign() error = %v", err)
	}

	// Try to verify with different message
	differentMessage := []byte("different message")
	err = adr036.VerifySignature(kp.PubKey.Bytes(), differentMessage, sig, kp.Address)
	if err == nil {
		t.Error("VerifySignature() = nil, want error for wrong message")
	}
}

func TestVerifySignature_WrongSigner(t *testing.T) {
	kp := testutil.NewTestKeyPair("test-seed-4")

	message := []byte("test message")
	// Sign with correct signer
	signBytes := adr036.CreateSignBytes(message, kp.Address)
	sig, err := kp.PrivKey.Sign(signBytes)
	if err != nil {
		t.Fatalf("Sign() error = %v", err)
	}

	// Verify with wrong signer address
	err = adr036.VerifySignature(kp.PubKey.Bytes(), message, sig, "manifest1wrongaddress")
	if err == nil {
		t.Error("VerifySignature() = nil, want error for wrong signer")
	}
}

func TestCreateSignBytes(t *testing.T) {
	message := []byte("hello world")
	signer := "manifest1abc123"

	signBytes := adr036.CreateSignBytes(message, signer)
	if signBytes == nil {
		t.Fatal("CreateSignBytes() = nil")
	}

	// Verify it's valid JSON
	var doc adr036.SignDoc
	if err := json.Unmarshal(signBytes, &doc); err != nil {
		t.Fatalf("CreateSignBytes() produced invalid JSON: %v", err)
	}

	// Verify structure
	if doc.AccountNumber != "0" {
		t.Errorf("AccountNumber = %q, want %q", doc.AccountNumber, "0")
	}
	if doc.ChainID != "" {
		t.Errorf("ChainID = %q, want empty string", doc.ChainID)
	}
	if doc.Sequence != "0" {
		t.Errorf("Sequence = %q, want %q", doc.Sequence, "0")
	}
	if doc.Fee.Gas != "0" {
		t.Errorf("Fee.Gas = %q, want %q", doc.Fee.Gas, "0")
	}
	if len(doc.Fee.Amount) != 0 {
		t.Errorf("Fee.Amount length = %d, want 0", len(doc.Fee.Amount))
	}
	if len(doc.Msgs) != 1 {
		t.Fatalf("Msgs length = %d, want 1", len(doc.Msgs))
	}
	if doc.Msgs[0].Type != "sign/MsgSignData" {
		t.Errorf("Msgs[0].Type = %q, want %q", doc.Msgs[0].Type, "sign/MsgSignData")
	}
	if doc.Msgs[0].Value.Signer != signer {
		t.Errorf("Msgs[0].Value.Signer = %q, want %q", doc.Msgs[0].Value.Signer, signer)
	}

	// Verify data is base64 encoded message
	decodedData, err := base64.StdEncoding.DecodeString(doc.Msgs[0].Value.Data)
	if err != nil {
		t.Fatalf("Failed to decode data: %v", err)
	}
	if string(decodedData) != string(message) {
		t.Errorf("Decoded data = %q, want %q", string(decodedData), string(message))
	}
}

func TestCreateSignDoc(t *testing.T) {
	message := []byte("test data")
	signer := "manifest1xyz789"

	doc := adr036.CreateSignDoc(message, signer)
	if doc == nil {
		t.Fatal("CreateSignDoc() = nil")
	}

	if doc.AccountNumber != "0" {
		t.Errorf("AccountNumber = %q, want %q", doc.AccountNumber, "0")
	}
	if doc.ChainID != "" {
		t.Errorf("ChainID = %q, want empty string", doc.ChainID)
	}
	if doc.Memo != "" {
		t.Errorf("Memo = %q, want empty string", doc.Memo)
	}
	if len(doc.Msgs) != 1 {
		t.Fatalf("Msgs length = %d, want 1", len(doc.Msgs))
	}
	if doc.Msgs[0].Value.Signer != signer {
		t.Errorf("Signer = %q, want %q", doc.Msgs[0].Value.Signer, signer)
	}

	expectedData := base64.StdEncoding.EncodeToString(message)
	if doc.Msgs[0].Value.Data != expectedData {
		t.Errorf("Data = %q, want %q", doc.Msgs[0].Value.Data, expectedData)
	}
}

func TestCreateSignBytes_Deterministic(t *testing.T) {
	message := []byte("deterministic test")
	signer := "manifest1deterministic"

	// Call multiple times and verify same result
	result1 := adr036.CreateSignBytes(message, signer)
	result2 := adr036.CreateSignBytes(message, signer)
	result3 := adr036.CreateSignBytes(message, signer)

	if string(result1) != string(result2) || string(result2) != string(result3) {
		t.Error("CreateSignBytes() is not deterministic")
	}
}
