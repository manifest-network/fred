package adr036_test

import (
	"encoding/base64"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/adr036"
	"github.com/manifest-network/fred/internal/testutil"
)

func TestVerifySignature_Valid(t *testing.T) {
	kp := testutil.NewTestKeyPair("test-seed-1")

	message := []byte("test message")
	signBytes := adr036.CreateSignBytes(message, kp.Address)
	sig, err := kp.PrivKey.Sign(signBytes)
	require.NoError(t, err)

	err = adr036.VerifySignature(kp.PubKey.Bytes(), message, sig, kp.Address)
	assert.NoError(t, err)
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
			assert.Error(t, err)
			if err != nil && tt.pubKeyLen != 33 {
				assert.Contains(t, err.Error(), "invalid public key length")
			}
		})
	}
}

func TestVerifySignature_InvalidSignature(t *testing.T) {
	kp := testutil.NewTestKeyPair("test-seed-2")

	message := []byte("test message")
	wrongSig := make([]byte, 64) // Invalid signature

	err := adr036.VerifySignature(kp.PubKey.Bytes(), message, wrongSig, kp.Address)
	assert.Error(t, err)
}

func TestVerifySignature_WrongMessage(t *testing.T) {
	kp := testutil.NewTestKeyPair("test-seed-3")

	originalMessage := []byte("original message")
	signBytes := adr036.CreateSignBytes(originalMessage, kp.Address)
	sig, err := kp.PrivKey.Sign(signBytes)
	require.NoError(t, err)

	// Try to verify with different message
	differentMessage := []byte("different message")
	err = adr036.VerifySignature(kp.PubKey.Bytes(), differentMessage, sig, kp.Address)
	assert.Error(t, err)
}

func TestVerifySignature_WrongSigner(t *testing.T) {
	kp := testutil.NewTestKeyPair("test-seed-4")

	message := []byte("test message")
	// Sign with correct signer
	signBytes := adr036.CreateSignBytes(message, kp.Address)
	sig, err := kp.PrivKey.Sign(signBytes)
	require.NoError(t, err)

	// Verify with wrong signer address
	err = adr036.VerifySignature(kp.PubKey.Bytes(), message, sig, "manifest1wrongaddress")
	assert.Error(t, err)
}

func TestCreateSignBytes(t *testing.T) {
	message := []byte("hello world")
	signer := "manifest1abc123"

	signBytes := adr036.CreateSignBytes(message, signer)
	require.NotNil(t, signBytes)

	// Verify it's valid JSON
	var doc adr036.SignDoc
	err := json.Unmarshal(signBytes, &doc)
	require.NoError(t, err)

	// Verify structure
	assert.Equal(t, "0", doc.AccountNumber)
	assert.Equal(t, "", doc.ChainID)
	assert.Equal(t, "0", doc.Sequence)
	assert.Equal(t, "0", doc.Fee.Gas)
	assert.Len(t, doc.Fee.Amount, 0)
	require.Len(t, doc.Msgs, 1)
	assert.Equal(t, "sign/MsgSignData", doc.Msgs[0].Type)
	assert.Equal(t, signer, doc.Msgs[0].Value.Signer)

	// Verify data is base64 encoded message
	decodedData, err := base64.StdEncoding.DecodeString(doc.Msgs[0].Value.Data)
	require.NoError(t, err)
	assert.Equal(t, string(message), string(decodedData))
}

func TestCreateSignDoc(t *testing.T) {
	message := []byte("test data")
	signer := "manifest1xyz789"

	doc := adr036.CreateSignDoc(message, signer)
	require.NotNil(t, doc)

	assert.Equal(t, "0", doc.AccountNumber)
	assert.Equal(t, "", doc.ChainID)
	assert.Equal(t, "", doc.Memo)
	require.Len(t, doc.Msgs, 1)
	assert.Equal(t, signer, doc.Msgs[0].Value.Signer)

	expectedData := base64.StdEncoding.EncodeToString(message)
	assert.Equal(t, expectedData, doc.Msgs[0].Value.Data)
}

func TestCreateSignBytes_Deterministic(t *testing.T) {
	message := []byte("deterministic test")
	signer := "manifest1deterministic"

	// Call multiple times and verify same result
	result1 := adr036.CreateSignBytes(message, signer)
	result2 := adr036.CreateSignBytes(message, signer)
	result3 := adr036.CreateSignBytes(message, signer)

	assert.True(t, string(result1) == string(result2) && string(result2) == string(result3))
}
