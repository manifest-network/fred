package api

import (
	"encoding/base64"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/testutil"
)

func TestParsePayloadAuthToken_Valid(t *testing.T) {
	kp := testutil.NewTestKeyPair("payload-auth-test-1")
	leaseUUID := testutil.ValidUUID1
	metaHash := "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"

	tokenStr := testutil.CreateTestPayloadToken(kp, leaseUUID, metaHash, time.Now())

	token, err := ParsePayloadAuthToken(tokenStr)
	require.NoError(t, err)

	assert.Equal(t, kp.Address, token.Tenant)
	assert.Equal(t, leaseUUID, token.LeaseUUID)
	assert.Equal(t, metaHash, token.MetaHash)
	assert.Equal(t, kp.PubKeyB64, token.PubKey)
}

func TestParsePayloadAuthToken_InvalidBase64(t *testing.T) {
	invalidTokens := []string{
		"not-base64!!!",
		"====",
		"a]b[c",
	}

	for _, tokenStr := range invalidTokens {
		_, err := ParsePayloadAuthToken(tokenStr)
		assert.Error(t, err, "ParsePayloadAuthToken(%q) = nil error, want error", tokenStr)
	}
}

func TestPayloadAuthToken_Validate_ExpiredToken(t *testing.T) {
	kp := testutil.NewTestKeyPair("payload-auth-test-2")
	leaseUUID := testutil.ValidUUID1
	metaHash := "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"

	// Create token from 10 minutes ago (expired)
	tokenStr := testutil.CreateExpiredPayloadToken(kp, leaseUUID, metaHash)

	token, err := ParsePayloadAuthToken(tokenStr)
	require.NoError(t, err)

	err = token.Validate("manifest")
	assert.Error(t, err, "Validate() = nil, want error for expired token")
}

func TestPayloadAuthToken_Validate_FutureToken(t *testing.T) {
	kp := testutil.NewTestKeyPair("payload-auth-test-3")
	leaseUUID := testutil.ValidUUID1
	metaHash := "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"

	// Create token from 10 minutes in the future
	tokenStr := testutil.CreateFuturePayloadToken(kp, leaseUUID, metaHash)

	token, err := ParsePayloadAuthToken(tokenStr)
	require.NoError(t, err)

	err = token.Validate("manifest")
	assert.Error(t, err, "Validate() = nil, want error for future token")
}

func TestPayloadAuthToken_Validate_MissingMetaHash(t *testing.T) {
	kp := testutil.NewTestKeyPair("payload-auth-test-4")
	leaseUUID := testutil.ValidUUID1

	// Create token with empty meta_hash
	tokenStr := testutil.CreateTestPayloadToken(kp, leaseUUID, "", time.Now())

	token, err := ParsePayloadAuthToken(tokenStr)
	require.NoError(t, err)

	err = token.Validate("manifest")
	assert.Error(t, err, "Validate() = nil, want error for missing meta_hash")
}

func TestPayloadAuthToken_Validate_MissingLeaseUUID(t *testing.T) {
	// Create a token with empty lease_uuid
	tokenData := PayloadAuthToken{
		Tenant:    "manifest1test",
		LeaseUUID: "", // Empty
		MetaHash:  "abcdef0123456789",
		Timestamp: time.Now().Unix(),
		PubKey:    "dummy",
		Signature: "dummy",
	}

	err := tokenData.Validate("manifest")
	assert.Error(t, err, "Validate() = nil, want error for missing lease_uuid")
}

func TestPayloadAuthToken_Validate_MissingTenant(t *testing.T) {
	// Create a token with empty tenant
	tokenData := PayloadAuthToken{
		Tenant:    "", // Empty
		LeaseUUID: testutil.ValidUUID1,
		MetaHash:  "abcdef0123456789",
		Timestamp: time.Now().Unix(),
		PubKey:    "dummy",
		Signature: "dummy",
	}

	err := tokenData.Validate("manifest")
	assert.Error(t, err, "Validate() = nil, want error for missing tenant")
}

func TestPayloadAuthToken_Validate_InvalidPubKeyLength(t *testing.T) {
	// Create a token with invalid public key length
	tokenData := PayloadAuthToken{
		Tenant:    "manifest1test",
		LeaseUUID: testutil.ValidUUID1,
		MetaHash:  "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789",
		Timestamp: time.Now().Unix(),
		PubKey:    base64.StdEncoding.EncodeToString([]byte("short")),
		Signature: base64.StdEncoding.EncodeToString([]byte("fake-sig")),
	}

	err := tokenData.Validate("manifest")
	assert.Error(t, err, "Validate() = nil, want error for invalid pub key length")
}

func TestPayloadAuthToken_Validate_ValidToken(t *testing.T) {
	kp := testutil.NewTestKeyPair("payload-auth-test-5")
	leaseUUID := testutil.ValidUUID1
	metaHash := "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"

	tokenStr := testutil.CreateTestPayloadToken(kp, leaseUUID, metaHash, time.Now())

	token, err := ParsePayloadAuthToken(tokenStr)
	require.NoError(t, err)

	err = token.Validate("manifest")
	assert.NoError(t, err)
}

func TestPayloadAuthToken_Validate_WrongBech32Prefix(t *testing.T) {
	kp := testutil.NewTestKeyPair("payload-auth-test-6")
	leaseUUID := testutil.ValidUUID1
	metaHash := "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"

	// Token is created with "manifest" prefix address
	tokenStr := testutil.CreateTestPayloadToken(kp, leaseUUID, metaHash, time.Now())

	token, err := ParsePayloadAuthToken(tokenStr)
	require.NoError(t, err)

	// Validate with different prefix - should fail address verification
	err = token.Validate("cosmos")
	assert.Error(t, err, "Validate() = nil, want error for wrong bech32 prefix")
}

func TestPayloadAuthToken_CreateSignData(t *testing.T) {
	token := &PayloadAuthToken{
		Tenant:    "manifest1abc",
		LeaseUUID: "01234567-89ab-cdef-0123-456789abcdef",
		MetaHash:  "abcdef0123456789",
		Timestamp: 1234567890,
	}

	signData := token.createSignData()
	expected := "manifest lease data 01234567-89ab-cdef-0123-456789abcdef abcdef0123456789 1234567890"

	assert.Equal(t, expected, string(signData))
}
